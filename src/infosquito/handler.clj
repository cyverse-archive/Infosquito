(ns infosquito.handler
  "Provides the ring handler for Infosquito.  It uses compojure to route the 
   requests."
  (:require [compojure.core :as cc]
            [compojure.handler :as ch]
            [ring.middleware.head :as rmh]))

(defprotocol ROUTES
  "This defines the set of routes that need to be handled by an Infosquito 
   implementation.  This has been created to facilitate automating unit testing."
  
  (help [_]
    "provides API help

     RETURN
       either a ring response map or a string to put in a response body")

  (search [_ user name sort window]
    "performs the search

     PARAMETERS
       user - The name of the user who owns the home folder being searched.
       name - a glob representing the names of the interesting files.
       sort - the matching sorting method
       window - the index window used to filter the matches to return

     RETURN
       either a ring response map or a string to put in a response body")

  (unknown [_]
    "handles an unknown route

     RETURN
       either a ring response map or a string to put in a response body")

  (welcome [_]
    "identifies the application

     RETURN
       either a ring response map or a string to put in a response body"))
          
(defn mk-handler
  "constructs the ring handler that uses a given set of routes

   PARAMETERS
     routes - something that implements the ROUTES protocol.  The handler will
       delegate to this.
  
   RETURN
     a ring handler"
  [routes]
  (rmh/wrap-head 
    (ch/api 
      (cc/let-routes []
        (cc/GET "/"       [] (welcome routes))
        (cc/GET "/search" [u n sort window] (search routes u n sort window))
        (cc/OPTIONS "*"   [] (help routes))
        (cc/ANY "*"       [] (unknown routes))))))
