#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32) && defined(SAKURA_BPTREE_BUILDING_DLL)
#define SAKURA_BPTREE_API __declspec(dllexport)
#elif defined(_WIN32)
#define SAKURA_BPTREE_API __declspec(dllimport)
#else
#define SAKURA_BPTREE_API
#endif

#define SAKURA_SHA32_KEY_SIZE 32u

typedef struct sakura_bptree sakura_bptree;

typedef enum sakura_bptree_status {
    SAKURA_BPTREE_OK = 0,
    SAKURA_BPTREE_NOT_FOUND = 1,
    SAKURA_BPTREE_INVALID_ARGUMENT = 2,
    SAKURA_BPTREE_OUT_OF_MEMORY = 3,
    SAKURA_BPTREE_IO_ERROR = 4,
    SAKURA_BPTREE_CORRUPT_FILE = 5
} sakura_bptree_status;

typedef void (*sakura_bptree_iter_cb)(const uint8_t key[SAKURA_SHA32_KEY_SIZE],
                                      const void *value,
                                      uint64_t value_len,
                                      void *user_data);

#define BPTREE_ORDER 32
#define BPTREE_MAX_KEYS (BPTREE_ORDER - 1)
#define BPTREE_MAGIC "SKBPT1\0\0"
#define BPTREE_MAGIC_SIZE 8u
#define BPTREE_VERSION 1u

typedef struct bptree_value {
    uint8_t *data;
    uint64_t len;
} bptree_value;

typedef struct bptree_node {
    int is_leaf;
    int key_count;
    uint8_t keys[BPTREE_ORDER][SAKURA_SHA32_KEY_SIZE];
    struct bptree_node *parent;
    struct bptree_node *next;
    union {
        struct bptree_node *children[BPTREE_ORDER + 1];
        bptree_value values[BPTREE_ORDER];
    } slots;
} bptree_node;

struct sakura_bptree {
    bptree_node *root;
    bptree_node *first_leaf;
    uint64_t len;
};

static int key_cmp(const uint8_t a[SAKURA_SHA32_KEY_SIZE],
                   const uint8_t b[SAKURA_SHA32_KEY_SIZE])
{
    return memcmp(a, b, SAKURA_SHA32_KEY_SIZE);
}

static void key_copy(uint8_t dst[SAKURA_SHA32_KEY_SIZE],
                     const uint8_t src[SAKURA_SHA32_KEY_SIZE])
{
    memcpy(dst, src, SAKURA_SHA32_KEY_SIZE);
}

static bptree_node *node_create(int is_leaf)
{
    bptree_node *node = (bptree_node *)calloc(1, sizeof(bptree_node));
    if (node == NULL) {
        return NULL;
    }
    node->is_leaf = is_leaf;
    return node;
}

static void node_free(bptree_node *node)
{
    int i;

    if (node == NULL) {
        return;
    }

    if (node->is_leaf) {
        for (i = 0; i < node->key_count; i++) {
            free(node->slots.values[i].data);
        }
    } else {
        for (i = 0; i <= node->key_count; i++) {
            node_free(node->slots.children[i]);
        }
    }

    free(node);
}

static int lower_bound_keys(const uint8_t keys[][SAKURA_SHA32_KEY_SIZE],
                            int count,
                            const uint8_t key[SAKURA_SHA32_KEY_SIZE])
{
    int lo = 0;
    int hi = count;

    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (key_cmp(keys[mid], key) < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    return lo;
}

static int upper_bound_keys(const uint8_t keys[][SAKURA_SHA32_KEY_SIZE],
                            int count,
                            const uint8_t key[SAKURA_SHA32_KEY_SIZE])
{
    int lo = 0;
    int hi = count;

    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (key_cmp(keys[mid], key) <= 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    return lo;
}

static bptree_node *find_leaf(const sakura_bptree *tree,
                              const uint8_t key[SAKURA_SHA32_KEY_SIZE])
{
    bptree_node *node;

    if (tree == NULL || tree->root == NULL) {
        return NULL;
    }

    node = tree->root;
    while (!node->is_leaf) {
        int idx = upper_bound_keys(node->keys, node->key_count, key);
        node = node->slots.children[idx];
    }

    return node;
}

static void refresh_first_leaf(sakura_bptree *tree)
{
    bptree_node *node;

    if (tree == NULL || tree->root == NULL) {
        return;
    }

    node = tree->root;
    while (!node->is_leaf) {
        node = node->slots.children[0];
    }
    tree->first_leaf = node;
}

static int subtree_first_key(const bptree_node *node,
                             uint8_t out[SAKURA_SHA32_KEY_SIZE])
{
    while (node != NULL && !node->is_leaf) {
        node = node->slots.children[0];
    }
    if (node == NULL || node->key_count == 0) {
        return 0;
    }
    key_copy(out, node->keys[0]);
    return 1;
}

static void update_parent_separator(bptree_node *node)
{
    bptree_node *parent;
    uint8_t first_key[SAKURA_SHA32_KEY_SIZE];

    while (node != NULL && node->parent != NULL) {
        int i;
        parent = node->parent;
        for (i = 1; i <= parent->key_count; i++) {
            if (parent->slots.children[i] == node) {
                if (subtree_first_key(node, first_key)) {
                    key_copy(parent->keys[i - 1], first_key);
                }
                return;
            }
        }
        node = parent;
    }
}

static sakura_bptree_status value_make(bptree_value *slot,
                                       const void *value,
                                       uint64_t value_len)
{
    uint8_t *copy = NULL;

    if (value_len > 0 && value == NULL) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }

    if (value_len > 0) {
        if ((uint64_t)((size_t)value_len) != value_len) {
            return SAKURA_BPTREE_INVALID_ARGUMENT;
        }
        copy = (uint8_t *)malloc((size_t)value_len);
        if (copy == NULL) {
            return SAKURA_BPTREE_OUT_OF_MEMORY;
        }
        memcpy(copy, value, (size_t)value_len);
    }

    slot->data = copy;
    slot->len = value_len;
    return SAKURA_BPTREE_OK;
}

static sakura_bptree_status value_replace(bptree_value *slot,
                                          const void *value,
                                          uint64_t value_len)
{
    bptree_value next;
    sakura_bptree_status status;

    next.data = NULL;
    next.len = 0;
    status = value_make(&next, value, value_len);
    if (status != SAKURA_BPTREE_OK) {
        return status;
    }

    free(slot->data);
    *slot = next;
    return SAKURA_BPTREE_OK;
}

static sakura_bptree_status insert_into_parent(sakura_bptree *tree,
                                               bptree_node *left,
                                               const uint8_t key[SAKURA_SHA32_KEY_SIZE],
                                               bptree_node *right);

static sakura_bptree_status split_internal(sakura_bptree *tree, bptree_node *node)
{
    bptree_node *right;
    int split;
    int right_keys;
    int i;
    uint8_t promote[SAKURA_SHA32_KEY_SIZE];

    right = node_create(0);
    if (right == NULL) {
        return SAKURA_BPTREE_OUT_OF_MEMORY;
    }

    split = BPTREE_ORDER / 2;
    right_keys = node->key_count - split - 1;
    key_copy(promote, node->keys[split]);

    for (i = 0; i < right_keys; i++) {
        key_copy(right->keys[i], node->keys[split + 1 + i]);
    }
    for (i = 0; i <= right_keys; i++) {
        right->slots.children[i] = node->slots.children[split + 1 + i];
        if (right->slots.children[i] != NULL) {
            right->slots.children[i]->parent = right;
        }
        node->slots.children[split + 1 + i] = NULL;
    }

    right->key_count = right_keys;
    node->key_count = split;
    right->parent = node->parent;

    return insert_into_parent(tree, node, promote, right);
}

static sakura_bptree_status insert_into_parent(sakura_bptree *tree,
                                               bptree_node *left,
                                               const uint8_t key[SAKURA_SHA32_KEY_SIZE],
                                               bptree_node *right)
{
    bptree_node *parent;
    int left_index;
    int i;

    if (left->parent == NULL) {
        bptree_node *root = node_create(0);
        if (root == NULL) {
            return SAKURA_BPTREE_OUT_OF_MEMORY;
        }
        key_copy(root->keys[0], key);
        root->slots.children[0] = left;
        root->slots.children[1] = right;
        root->key_count = 1;
        left->parent = root;
        right->parent = root;
        tree->root = root;
        return SAKURA_BPTREE_OK;
    }

    parent = left->parent;
    left_index = 0;
    while (left_index <= parent->key_count && parent->slots.children[left_index] != left) {
        left_index++;
    }
    if (left_index > parent->key_count) {
        return SAKURA_BPTREE_CORRUPT_FILE;
    }

    for (i = parent->key_count; i > left_index; i--) {
        key_copy(parent->keys[i], parent->keys[i - 1]);
    }
    for (i = parent->key_count + 1; i > left_index + 1; i--) {
        parent->slots.children[i] = parent->slots.children[i - 1];
    }

    key_copy(parent->keys[left_index], key);
    parent->slots.children[left_index + 1] = right;
    right->parent = parent;
    parent->key_count++;

    if (parent->key_count <= BPTREE_MAX_KEYS) {
        return SAKURA_BPTREE_OK;
    }

    return split_internal(tree, parent);
}

static sakura_bptree_status split_leaf_and_insert(sakura_bptree *tree,
                                                  bptree_node *leaf,
                                                  const uint8_t key[SAKURA_SHA32_KEY_SIZE],
                                                  const void *value,
                                                  uint64_t value_len)
{
    uint8_t temp_keys[BPTREE_ORDER][SAKURA_SHA32_KEY_SIZE];
    bptree_value temp_values[BPTREE_ORDER];
    bptree_value new_value;
    bptree_node *right;
    int insert_at;
    int split;
    int right_count;
    int i;
    sakura_bptree_status status;

    memset(temp_values, 0, sizeof(temp_values));
    new_value.data = NULL;
    new_value.len = 0;
    status = value_make(&new_value, value, value_len);
    if (status != SAKURA_BPTREE_OK) {
        return status;
    }

    right = node_create(1);
    if (right == NULL) {
        free(new_value.data);
        return SAKURA_BPTREE_OUT_OF_MEMORY;
    }

    insert_at = lower_bound_keys(leaf->keys, leaf->key_count, key);

    for (i = 0; i < insert_at; i++) {
        key_copy(temp_keys[i], leaf->keys[i]);
        temp_values[i] = leaf->slots.values[i];
        leaf->slots.values[i].data = NULL;
        leaf->slots.values[i].len = 0;
    }
    key_copy(temp_keys[insert_at], key);
    temp_values[insert_at] = new_value;
    for (i = insert_at; i < leaf->key_count; i++) {
        key_copy(temp_keys[i + 1], leaf->keys[i]);
        temp_values[i + 1] = leaf->slots.values[i];
        leaf->slots.values[i].data = NULL;
        leaf->slots.values[i].len = 0;
    }

    split = BPTREE_ORDER / 2;
    leaf->key_count = split;
    right_count = BPTREE_ORDER - split;
    right->key_count = right_count;
    right->parent = leaf->parent;
    right->next = leaf->next;
    leaf->next = right;

    for (i = 0; i < split; i++) {
        key_copy(leaf->keys[i], temp_keys[i]);
        leaf->slots.values[i] = temp_values[i];
    }
    for (i = 0; i < right_count; i++) {
        key_copy(right->keys[i], temp_keys[split + i]);
        right->slots.values[i] = temp_values[split + i];
    }

    tree->len++;
    return insert_into_parent(tree, leaf, right->keys[0], right);
}

SAKURA_BPTREE_API sakura_bptree *sakura_bptree_create(void)
{
    sakura_bptree *tree = (sakura_bptree *)calloc(1, sizeof(sakura_bptree));
    if (tree == NULL) {
        return NULL;
    }

    tree->root = node_create(1);
    if (tree->root == NULL) {
        free(tree);
        return NULL;
    }
    tree->first_leaf = tree->root;
    return tree;
}

SAKURA_BPTREE_API void sakura_bptree_free(sakura_bptree *tree)
{
    if (tree == NULL) {
        return;
    }
    node_free(tree->root);
    free(tree);
}

SAKURA_BPTREE_API uint64_t sakura_bptree_len(const sakura_bptree *tree)
{
    return tree == NULL ? 0 : tree->len;
}

SAKURA_BPTREE_API sakura_bptree_status sakura_bptree_put(sakura_bptree *tree,
                                                         const uint8_t key[SAKURA_SHA32_KEY_SIZE],
                                                         const void *value,
                                                         uint64_t value_len)
{
    bptree_node *leaf;
    int pos;
    int i;
    sakura_bptree_status status;

    if (tree == NULL || key == NULL || (value == NULL && value_len > 0)) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }

    leaf = find_leaf(tree, key);
    if (leaf == NULL) {
        return SAKURA_BPTREE_CORRUPT_FILE;
    }

    pos = lower_bound_keys(leaf->keys, leaf->key_count, key);
    if (pos < leaf->key_count && key_cmp(leaf->keys[pos], key) == 0) {
        return value_replace(&leaf->slots.values[pos], value, value_len);
    }

    if (leaf->key_count == BPTREE_MAX_KEYS) {
        return split_leaf_and_insert(tree, leaf, key, value, value_len);
    }

    for (i = leaf->key_count; i > pos; i--) {
        key_copy(leaf->keys[i], leaf->keys[i - 1]);
        leaf->slots.values[i] = leaf->slots.values[i - 1];
    }

    key_copy(leaf->keys[pos], key);
    leaf->slots.values[pos].data = NULL;
    leaf->slots.values[pos].len = 0;
    status = value_make(&leaf->slots.values[pos], value, value_len);
    if (status != SAKURA_BPTREE_OK) {
        return status;
    }

    leaf->key_count++;
    tree->len++;
    if (pos == 0) {
        update_parent_separator(leaf);
    }

    return SAKURA_BPTREE_OK;
}

SAKURA_BPTREE_API sakura_bptree_status sakura_bptree_get(const sakura_bptree *tree,
                                                         const uint8_t key[SAKURA_SHA32_KEY_SIZE],
                                                         const void **value,
                                                         uint64_t *value_len)
{
    bptree_node *leaf;
    int pos;

    if (tree == NULL || key == NULL || value == NULL || value_len == NULL) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }

    leaf = find_leaf(tree, key);
    if (leaf == NULL) {
        return SAKURA_BPTREE_NOT_FOUND;
    }

    pos = lower_bound_keys(leaf->keys, leaf->key_count, key);
    if (pos >= leaf->key_count || key_cmp(leaf->keys[pos], key) != 0) {
        return SAKURA_BPTREE_NOT_FOUND;
    }

    *value = leaf->slots.values[pos].data;
    *value_len = leaf->slots.values[pos].len;
    return SAKURA_BPTREE_OK;
}

SAKURA_BPTREE_API sakura_bptree_status sakura_bptree_copy_value(const sakura_bptree *tree,
                                                                const uint8_t key[SAKURA_SHA32_KEY_SIZE],
                                                                void *buffer,
                                                                uint64_t buffer_len,
                                                                uint64_t *value_len)
{
    const void *value;
    uint64_t len;
    sakura_bptree_status status;

    if (value_len == NULL || (buffer == NULL && buffer_len > 0)) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }

    status = sakura_bptree_get(tree, key, &value, &len);
    if (status != SAKURA_BPTREE_OK) {
        return status;
    }

    *value_len = len;
    if (buffer_len < len) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }
    if (len > 0) {
        memcpy(buffer, value, (size_t)len);
    }
    return SAKURA_BPTREE_OK;
}

SAKURA_BPTREE_API sakura_bptree_status sakura_bptree_remove(sakura_bptree *tree,
                                                            const uint8_t key[SAKURA_SHA32_KEY_SIZE])
{
    bptree_node *leaf;
    int pos;
    int i;

    if (tree == NULL || key == NULL) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }

    leaf = find_leaf(tree, key);
    if (leaf == NULL) {
        return SAKURA_BPTREE_NOT_FOUND;
    }
    pos = lower_bound_keys(leaf->keys, leaf->key_count, key);
    if (pos >= leaf->key_count || key_cmp(leaf->keys[pos], key) != 0) {
        return SAKURA_BPTREE_NOT_FOUND;
    }

    free(leaf->slots.values[pos].data);
    for (i = pos; i < leaf->key_count - 1; i++) {
        key_copy(leaf->keys[i], leaf->keys[i + 1]);
        leaf->slots.values[i] = leaf->slots.values[i + 1];
    }
    leaf->slots.values[leaf->key_count - 1].data = NULL;
    leaf->slots.values[leaf->key_count - 1].len = 0;
    leaf->key_count--;
    tree->len--;

    if (leaf->key_count > 0 && pos == 0) {
        update_parent_separator(leaf);
    }

    return SAKURA_BPTREE_OK;
}

SAKURA_BPTREE_API void sakura_bptree_iterate(const sakura_bptree *tree,
                                             sakura_bptree_iter_cb callback,
                                             void *user_data)
{
    bptree_node *node;

    if (tree == NULL || callback == NULL) {
        return;
    }

    node = tree->first_leaf;
    while (node != NULL) {
        int i;
        for (i = 0; i < node->key_count; i++) {
            callback(node->keys[i], node->slots.values[i].data,
                     node->slots.values[i].len, user_data);
        }
        node = node->next;
    }
}

typedef struct buf_writer {
    uint8_t *data;
    uint64_t len;
    uint64_t cap;
} buf_writer;

static int buf_writer_init(buf_writer *w)
{
    w->data = (uint8_t *)malloc(256);
    if (w->data == NULL) {
        return 0;
    }
    w->len = 0;
    w->cap = 256;
    return 1;
}

static int buf_writer_write(buf_writer *w, const void *src, uint64_t n)
{
    uint64_t needed = w->len + n;
    if (needed > w->cap) {
        uint64_t new_cap = w->cap * 2;
        uint8_t *p;
        while (new_cap < needed) {
            new_cap *= 2;
        }
        p = (uint8_t *)realloc(w->data, (size_t)new_cap);
        if (p == NULL) {
            return 0;
        }
        w->data = p;
        w->cap = new_cap;
    }
    memcpy(w->data + w->len, src, (size_t)n);
    w->len += n;
    return 1;
}

typedef struct buf_reader {
    const uint8_t *data;
    uint64_t len;
    uint64_t pos;
} buf_reader;

static int buf_reader_read(buf_reader *r, void *dst, uint64_t n)
{
    if (r->pos + n > r->len) {
        return 0;
    }
    memcpy(dst, r->data + r->pos, (size_t)n);
    r->pos += n;
    return 1;
}

static int buf_write_u32(buf_writer *w, uint32_t value)
{
    uint8_t b[4];
    b[0] = (uint8_t)(value & 0xffu);
    b[1] = (uint8_t)((value >> 8) & 0xffu);
    b[2] = (uint8_t)((value >> 16) & 0xffu);
    b[3] = (uint8_t)((value >> 24) & 0xffu);
    return buf_writer_write(w, b, sizeof(b));
}

static int buf_write_u64(buf_writer *w, uint64_t value)
{
    uint8_t b[8];
    int i;
    for (i = 0; i < 8; i++) {
        b[i] = (uint8_t)((value >> (8 * i)) & 0xffu);
    }
    return buf_writer_write(w, b, sizeof(b));
}

static int buf_read_u32(buf_reader *r, uint32_t *value)
{
    uint8_t b[4];
    if (!buf_reader_read(r, b, sizeof(b))) {
        return 0;
    }
    *value = ((uint32_t)b[0]) | ((uint32_t)b[1] << 8) |
             ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
    return 1;
}

static int buf_read_u64(buf_reader *r, uint64_t *value)
{
    uint8_t b[8];
    int i;
    if (!buf_reader_read(r, b, sizeof(b))) {
        return 0;
    }
    *value = 0;
    for (i = 0; i < 8; i++) {
        *value |= ((uint64_t)b[i]) << (8 * i);
    }
    return 1;
}

static int write_u32(FILE *file, uint32_t value)
{
    uint8_t b[4];
    b[0] = (uint8_t)(value & 0xffu);
    b[1] = (uint8_t)((value >> 8) & 0xffu);
    b[2] = (uint8_t)((value >> 16) & 0xffu);
    b[3] = (uint8_t)((value >> 24) & 0xffu);
    return fwrite(b, 1, sizeof(b), file) == sizeof(b);
}

static int write_u64(FILE *file, uint64_t value)
{
    uint8_t b[8];
    int i;
    for (i = 0; i < 8; i++) {
        b[i] = (uint8_t)((value >> (8 * i)) & 0xffu);
    }
    return fwrite(b, 1, sizeof(b), file) == sizeof(b);
}

static int read_u32(FILE *file, uint32_t *value)
{
    uint8_t b[4];
    if (fread(b, 1, sizeof(b), file) != sizeof(b)) {
        return 0;
    }
    *value = ((uint32_t)b[0]) | ((uint32_t)b[1] << 8) |
             ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
    return 1;
}

static int read_u64(FILE *file, uint64_t *value)
{
    uint8_t b[8];
    int i;

    if (fread(b, 1, sizeof(b), file) != sizeof(b)) {
        return 0;
    }

    *value = 0;
    for (i = 0; i < 8; i++) {
        *value |= ((uint64_t)b[i]) << (8 * i);
    }
    return 1;
}

SAKURA_BPTREE_API sakura_bptree_status sakura_bptree_save(const sakura_bptree *tree,
                                                          const char *path)
{
    FILE *file;
    bptree_node *node;

    if (tree == NULL || path == NULL) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }

    file = fopen(path, "wb");
    if (file == NULL) {
        return SAKURA_BPTREE_IO_ERROR;
    }

    if (fwrite(BPTREE_MAGIC, 1, BPTREE_MAGIC_SIZE, file) != BPTREE_MAGIC_SIZE ||
        !write_u32(file, BPTREE_VERSION) ||
        !write_u32(file, SAKURA_SHA32_KEY_SIZE) ||
        !write_u64(file, tree->len)) {
        fclose(file);
        return SAKURA_BPTREE_IO_ERROR;
    }

    node = tree->first_leaf;
    while (node != NULL) {
        int i;
        for (i = 0; i < node->key_count; i++) {
            if (node->slots.values[i].len > 0 &&
                (uint64_t)((size_t)node->slots.values[i].len) != node->slots.values[i].len) {
                fclose(file);
                return SAKURA_BPTREE_INVALID_ARGUMENT;
            }
            if (fwrite(node->keys[i], 1, SAKURA_SHA32_KEY_SIZE, file) != SAKURA_SHA32_KEY_SIZE ||
                !write_u64(file, node->slots.values[i].len) ||
                (node->slots.values[i].len > 0 &&
                 fwrite(node->slots.values[i].data, 1, (size_t)node->slots.values[i].len, file) !=
                     (size_t)node->slots.values[i].len)) {
                fclose(file);
                return SAKURA_BPTREE_IO_ERROR;
            }
        }
        node = node->next;
    }

    if (fclose(file) != 0) {
        return SAKURA_BPTREE_IO_ERROR;
    }
    return SAKURA_BPTREE_OK;
}

SAKURA_BPTREE_API sakura_bptree_status sakura_bptree_load(const char *path,
                                                          sakura_bptree **tree_out)
{
    FILE *file;
    uint8_t magic[BPTREE_MAGIC_SIZE];
    uint32_t version;
    uint32_t key_size;
    uint64_t count;
    uint64_t i;
    sakura_bptree *tree;

    if (path == NULL || tree_out == NULL) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }
    *tree_out = NULL;

    file = fopen(path, "rb");
    if (file == NULL) {
        return SAKURA_BPTREE_IO_ERROR;
    }

    if (fread(magic, 1, sizeof(magic), file) != sizeof(magic) ||
        memcmp(magic, BPTREE_MAGIC, BPTREE_MAGIC_SIZE) != 0 ||
        !read_u32(file, &version) ||
        !read_u32(file, &key_size) ||
        !read_u64(file, &count) ||
        version != BPTREE_VERSION ||
        key_size != SAKURA_SHA32_KEY_SIZE) {
        fclose(file);
        return SAKURA_BPTREE_CORRUPT_FILE;
    }

    tree = sakura_bptree_create();
    if (tree == NULL) {
        fclose(file);
        return SAKURA_BPTREE_OUT_OF_MEMORY;
    }

    for (i = 0; i < count; i++) {
        uint8_t key[SAKURA_SHA32_KEY_SIZE];
        uint64_t value_len;
        uint8_t *value = NULL;
        sakura_bptree_status status;

        if (fread(key, 1, SAKURA_SHA32_KEY_SIZE, file) != SAKURA_SHA32_KEY_SIZE ||
            !read_u64(file, &value_len) ||
            (value_len > 0 && (uint64_t)((size_t)value_len) != value_len)) {
            sakura_bptree_free(tree);
            fclose(file);
            return SAKURA_BPTREE_CORRUPT_FILE;
        }
        if (value_len > 0) {
            value = (uint8_t *)malloc((size_t)value_len);
            if (value == NULL) {
                sakura_bptree_free(tree);
                fclose(file);
                return SAKURA_BPTREE_OUT_OF_MEMORY;
            }
            if (fread(value, 1, (size_t)value_len, file) != (size_t)value_len) {
                free(value);
                sakura_bptree_free(tree);
                fclose(file);
                return SAKURA_BPTREE_CORRUPT_FILE;
            }
        }

        status = sakura_bptree_put(tree, key, value, value_len);
        free(value);
        if (status != SAKURA_BPTREE_OK) {
            sakura_bptree_free(tree);
            fclose(file);
            return status;
        }
    }

    if (fgetc(file) != EOF) {
        sakura_bptree_free(tree);
        fclose(file);
        return SAKURA_BPTREE_CORRUPT_FILE;
    }

    fclose(file);
    refresh_first_leaf(tree);
    *tree_out = tree;
    return SAKURA_BPTREE_OK;
}

SAKURA_BPTREE_API sakura_bptree_status sakura_bptree_serialize(const sakura_bptree *tree,
                                                               uint8_t **out,
                                                               uint64_t *out_len)
{
    buf_writer w;
    bptree_node *node;

    if (tree == NULL || out == NULL || out_len == NULL) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }

    if (!buf_writer_init(&w)) {
        return SAKURA_BPTREE_OUT_OF_MEMORY;
    }

    if (!buf_writer_write(&w, BPTREE_MAGIC, BPTREE_MAGIC_SIZE) ||
        !buf_write_u32(&w, BPTREE_VERSION) ||
        !buf_write_u32(&w, SAKURA_SHA32_KEY_SIZE) ||
        !buf_write_u64(&w, tree->len)) {
        free(w.data);
        return SAKURA_BPTREE_OUT_OF_MEMORY;
    }

    node = tree->first_leaf;
    while (node != NULL) {
        int i;
        for (i = 0; i < node->key_count; i++) {
            uint64_t vlen = node->slots.values[i].len;
            if (!buf_writer_write(&w, node->keys[i], SAKURA_SHA32_KEY_SIZE) ||
                !buf_write_u64(&w, vlen) ||
                (vlen > 0 && !buf_writer_write(&w, node->slots.values[i].data, vlen))) {
                free(w.data);
                return SAKURA_BPTREE_OUT_OF_MEMORY;
            }
        }
        node = node->next;
    }

    *out = w.data;
    *out_len = w.len;
    return SAKURA_BPTREE_OK;
}

SAKURA_BPTREE_API sakura_bptree_status sakura_bptree_deserialize(const uint8_t *data,
                                                                  uint64_t data_len,
                                                                  sakura_bptree **tree_out)
{
    buf_reader r;
    uint8_t magic[BPTREE_MAGIC_SIZE];
    uint32_t version;
    uint32_t key_size;
    uint64_t count;
    uint64_t i;
    sakura_bptree *tree;

    if (data == NULL || tree_out == NULL) {
        return SAKURA_BPTREE_INVALID_ARGUMENT;
    }
    *tree_out = NULL;

    r.data = data;
    r.len = data_len;
    r.pos = 0;

    if (!buf_reader_read(&r, magic, BPTREE_MAGIC_SIZE) ||
        memcmp(magic, BPTREE_MAGIC, BPTREE_MAGIC_SIZE) != 0 ||
        !buf_read_u32(&r, &version) ||
        !buf_read_u32(&r, &key_size) ||
        !buf_read_u64(&r, &count) ||
        version != BPTREE_VERSION ||
        key_size != SAKURA_SHA32_KEY_SIZE) {
        return SAKURA_BPTREE_CORRUPT_FILE;
    }

    tree = sakura_bptree_create();
    if (tree == NULL) {
        return SAKURA_BPTREE_OUT_OF_MEMORY;
    }

    for (i = 0; i < count; i++) {
        uint8_t key[SAKURA_SHA32_KEY_SIZE];
        uint64_t value_len;
        uint8_t *value = NULL;
        sakura_bptree_status status;

        if (!buf_reader_read(&r, key, SAKURA_SHA32_KEY_SIZE) ||
            !buf_read_u64(&r, &value_len) ||
            (value_len > 0 && (uint64_t)((size_t)value_len) != value_len)) {
            sakura_bptree_free(tree);
            return SAKURA_BPTREE_CORRUPT_FILE;
        }
        if (value_len > 0) {
            value = (uint8_t *)malloc((size_t)value_len);
            if (value == NULL) {
                sakura_bptree_free(tree);
                return SAKURA_BPTREE_OUT_OF_MEMORY;
            }
            if (!buf_reader_read(&r, value, value_len)) {
                free(value);
                sakura_bptree_free(tree);
                return SAKURA_BPTREE_CORRUPT_FILE;
            }
        }

        status = sakura_bptree_put(tree, key, value, value_len);
        free(value);
        if (status != SAKURA_BPTREE_OK) {
            sakura_bptree_free(tree);
            return status;
        }
    }

    if (r.pos != r.len) {
        sakura_bptree_free(tree);
        return SAKURA_BPTREE_CORRUPT_FILE;
    }

    refresh_first_leaf(tree);
    *tree_out = tree;
    return SAKURA_BPTREE_OK;
}

SAKURA_BPTREE_API const char *sakura_bptree_status_string(sakura_bptree_status status)
{
    switch (status) {
    case SAKURA_BPTREE_OK:
        return "ok";
    case SAKURA_BPTREE_NOT_FOUND:
        return "not found";
    case SAKURA_BPTREE_INVALID_ARGUMENT:
        return "invalid argument";
    case SAKURA_BPTREE_OUT_OF_MEMORY:
        return "out of memory";
    case SAKURA_BPTREE_IO_ERROR:
        return "io error";
    case SAKURA_BPTREE_CORRUPT_FILE:
        return "corrupt file";
    default:
        return "unknown";
    }
}
