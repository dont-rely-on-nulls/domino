#include <stdio.h>
#include <stdlib.h>
#include <lmdb.h>

int main(void) {
    MDB_env *env;
    int rc;

    rc = mdb_env_create(&env);
    if (rc) {
        fprintf(stderr, "mdb_env_create failed: %s\n", mdb_strerror(rc));
        return EXIT_FAILURE;
    }

    rc = mdb_env_open(env, "./testdb", MDB_NOSUBDIR, 0664);
    if (rc) {
        fprintf(stderr, "mdb_env_open failed: %s\n", mdb_strerror(rc));
        mdb_env_close(env);
        return EXIT_FAILURE;
    }

    printf("LMDB environment opened successfully.\n");
    printf("LMDB version: %s\n", mdb_version(NULL, NULL, NULL));

    mdb_env_close(env);
    return EXIT_SUCCESS;
}
