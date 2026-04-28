(prl (LoadLibrary "C:/Users/mague/source/repos/_build/default/plugin/lib/plugin.cmxs"))
(prl (DefineFunctionPredicate ((name "http.get") (schema (("url" "string") ("status_code" "string") ("body" "string"))) (symbol "http.get") (purity Pure) (cardinality ConstrainedFinite))))
(drl (Select (Const (("url" (Str "https://example.com")))) (Base "public:http.get")))
(drl (Join (url) (Const (("url" (Str "https://json.org/example.html")))) (Base "public:http.get")))
