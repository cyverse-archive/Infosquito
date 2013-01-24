(ns infosquito.main
  "This namespace defines the entry point for Infosquito."
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :as ss])
  (:import [java.util Properties]
           [org.apache.log4j PropertyConfigurator]))


(defn- configure-logger!
  [instance]
  (let [prop-file "log4j.template"]
    (if-let [stream (ClassLoader/getSystemResourceAsStream prop-file)]
      (PropertyConfigurator/configure (doto (Properties.)
                                        (.load stream)
                                        (.put "instance" instance)))
      (ss/throw+ {:type :cfg-problem :msg (str prop-file " cannot be found")}))))


(defn- parse-args
  [args]
  (cli/cli args
    ["-c" "--config" "sets the local configuration file to be read, bypassing Zookeeper"]
    ["-h" "--help" "show help and exit"
          :flag true]
    ["-i" "--id" "This is the ID of the infosquito instance"
          :default "anonymous"]))


(defn- run
  [opts]
  (require 'infosquito.core)
  (eval (if-let [config (:config opts)]
            `(infosquito.core/run-local ~config)
            `(infosquito.core/run-zookeeper))))


(defn -main
  [& args]
  (let [[opts _ help-str] (parse-args args)]
    (when (:help opts)
      (println help-str)
      (System/exit 0))
    (configure-logger! (:id opts))
    (ss/try+
      (run opts)
      (catch [:type :cfg-problem] {:keys [msg]}
        (log/error "There was a problem loading the configuration values." msg "Exiting."))
      (catch Object _ (log/error (:throwable &throw-context) "UNEXPECTED ERROR - EXITING")))))
