(ns infosquito.core
  "This namespace defines the entry point for Infosquito.  All state should be
   in here."
  (:gen-class)
  (:use infosquito.handler 
        infosquito.routes)
  (:require [clojure-commons.clavin-client :as cc]
            [clojure-commons.props :as cp]
            [clojure.tools.cli :as tc] 
            [clojure.tools.logging :as tl]
            [ring.adapter.jetty :as raj]))

(defn- parse-args
  [args]
  (tc/cli args
    ["-c" "--config"
     "sets the local configuration file to read, bypassing Zookeeper"
     :default nil]
    ["-h" "--help"
     "show help and exit"
     :flag true
     :default false]))

(defn- get-remote-props 
  []
  (let [zkprops (cp/parse-properties "zkhosts.properties")]
    (cc/with-zk (get zkprops "zookeeper")
      (if (cc/can-run?)
        (cc/properties "infosquito")
        (throw 
          (Exception. 
            "THIS APPLICATION CANNOT RUN ON THIS MACHINE.  SO SAYETH ZOOKEEPER."))))))

(defn- run-service 
  [cfg-file-path]
  (try
    (let [props       (if cfg-file-path
                        (cp/read-properties cfg-file-path)
                        (get-remote-props))
          listen-port (Integer. (get props "infosquito.app.listen-port"))
          help-url    (get props "infosquito.app.help-url")]
      (tl/info "Initializing on port" listen-port)
      (raj/run-jetty (mk-handler (mk-routes help-url)) {:port listen-port}))
    (catch Exception e
      (tl/error "Exiting," (.getMessage e)))))
  
(defn -main
  [& args]
  (let [[opts _ help-str] (parse-args args)]
    (if (:help opts) 
      (println help-str)
      (run-service (:config opts)))))
