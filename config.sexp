(server
 (nt (sqlite "/tmp/sakura.db"))
 (transport (tcp
             (address "127.0.0.1")
             (port 7777))))
