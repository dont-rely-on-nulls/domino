(lang dune 3.16)
(map_workspace_root false)

(using ctypes 0.3)

(name sakura)

(generate_opam_files true)

(source
 (github username/reponame))

(authors "Author Name")

(maintainers "Maintainer Name")

(license LICENSE)

(documentation https://url/to/documentation)

(package
  (allow_empty)
  (name sakura)
  (synopsis "A short synopsis")
  (description "A longer description")
  (depends
   ocaml
   dune
   ppx_protocol_conv
   ppx_protocol_conv_xml_light
   sha
   data-encoding
   (ppx_inline_test :with-test)
   ppx_deriving
   ppx_sexp_conv
   sexplib
   lwt
   lwt-exit
   batteries
   num
   (odoc :with-doc))
  (tags
   (topics "to describe" your project)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/reference/dune-project/index.html
