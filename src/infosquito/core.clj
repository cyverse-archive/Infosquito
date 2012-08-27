(ns infosquito.core
  "This namespace defines the entry point for Infosquito.  All state should be
   in here."
  (:gen-class)
  (:use infosquito.handler 
        infosquito.routes)
  (:require [clojure.tools.logging :as tl]
            [ring.adapter.jetty :as raj]))

; This will enventually get loaded from zookeeper or passed in on the command 
; line.
(def context ^{:private true}
  {:listen-port 8080
   :help-url    "https://github.com/iPlantCollaborativeOpenSource/Infosquito"})

(defn -main
  [& args]
  (let [listen-port (:listen-port context)]
    (tl/info "Initializing on port" listen-port)
    (raj/run-jetty 
      (mk-handler (.Routes (:help-url context))) 
      {:port listen-port})))
