;; Sakura server configuration
;; storage: memory
;; transport: tcp (address, port)
(server
 ;; (storage ((memory (multigroups (animal_crossing)))
 ;; 	   (lmdb (multigroups (luigi mario)))))
 (storage (memory))
 (transport (tcp
             (address "127.0.0.1")
             (port 7777))))
