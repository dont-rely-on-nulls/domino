;; Sakura server configuration
;; storage: memory
;; transport: tcp (address, port)
(server
(storage (memory))
 (transport (tcp
             (address "127.0.0.1")
             (port 7777))))
