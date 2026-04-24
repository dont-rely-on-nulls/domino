# 1. Clean
Remove-Item *.cmi, *.cmx, *.obj, *.o, *.exe, *.cmxs -ErrorAction SilentlyContinue

# 2. Interface (The Contract)
ocamlopt.exe -c plugin_intf.ml

# 3. Host (Link the interface code IN)
ocamlopt.exe -I +dynlink dynlink.cmxa plugin_intf.cmx main.ml -o app.exe

# 4. Plugin (DO NOT link plugin_intf.cmx here)
# Just use -I . so it finds plugin_intf.cmi for type checking
ocamlopt.exe -shared -I . -o my_plugin.cmxs my_plugin.ml

# 5. Run
.\app.exe