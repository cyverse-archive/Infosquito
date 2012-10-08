(ns infosquito.core
  "This namespace defines the entry point for Infosquito.  All state should be
   in here."
  (:gen-class)
  (:require [clojure.tools.cli :as tc] 
            [clojure.tools.logging :as tl]
            [com.github.drsnyder.beanstalk :as cgdb]
            [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as cj]
            [clojure-commons.clavin-client :as cc]
            [clojure-commons.props :as cp]
            [infosquito.worker :as w])
  (:import [java.net URL]))


(defn- init-irods
  [props]
  (cj/init (get props "infosquito.irods.host") 
           (get props "infosquito.irods.port")
           (get props "infosquito.irods.user")
           (get props "infosquito.irods.home")
           (get props "infosquito.irods.password")
           (get props "infosquito.irods.zone")
           (get props "infosquito.irods.default-resource")))


(defn- mk-beanstalk-ctor
  [props]
  #(cgdb/new-beanstalk (get props "infosquito.beanstalk.host")
                       (Integer/parseInt (get props "infosquito.beanstalk.port"))))


(defn- mk-es-url
  [props]
  (str (URL. "http" 
             (get props "infosquito.es.host") 
             (Integer/parseInt (get props "infosquito.es.port"))
             "")))


(defn- run 
  [mode props]
  (ss/try+      
    (let [worker (w/mk-worker (init-irods props)
                              (mk-beanstalk-ctor props)
                              (mk-es-url props)
                              (get props "infosquito.beanstalk.task-ttr"))]
      (condp = mode
        :passive (dorun (repeatedly #(w/process-next-task worker)))
        :sync    (w/sync-index worker)))
    (catch Exception e
      (tl/error "Exiting," (.getMessage e)))))
  

(defn- get-local-props
  [cfg-file]
  (ss/try+
    (cp/read-properties cfg-file)
    (catch Object _
      (ss/throw+ {:type :cfg-problem :cfg-file cfg-file}))))


(defn- get-remote-props 
  []
  (let [zkprops (cp/parse-properties "zkhosts.properties")]
    (cc/with-zk (get zkprops "zookeeper")
      (if (cc/can-run?)
        (cc/properties "infosquito")
        (ss/throw+ {:type :zk-perm})))))

  
(defn- map-mode
  [mode-str]
  (condp = mode-str
    "passive" :passive
    "sync"    :sync
              (ss/throw+ {:type :invalid-mode :mode mode-str})))


(defn- parse-args
  [args]
  (tc/cli args
    ["-c" "--config" 
     "sets the local configuration file to be read, bypassing Zookeeper"]
    ["-h" "--help" 
     "show help and exit"
     :flag true]
    ["-m" "--mode" 
     "Indicates how infosquito should be run. (passive|sync)"
     :default "passive"]))


(defn -main
  [& args]
  (ss/try+
    (let [[opts _ help-str] (parse-args args)]
      (if (:help opts) 
        (println help-str)
        (run (map-mode (:mode opts))
             (if-let [cfg-file (:config opts)]
               (get-local-props cfg-file)
               (get-remote-props)))))
    (catch [:type :invalid-mode] {:keys [mode]}
      (tl/error "Invalid mode, " mode))
    (catch [:type :cfg-problem] {:keys [cfg-file]}
      (tl/error "There was problem reading the configuration file" cfg-file "."))
    (catch [:type :zk-perm] {:keys []}
      (tl/error 
        "This application cannot run on this machine.  So sayeth Zookeeper."))
    (catch Object _
      (tl/error (:throwable &throw-context) "unexpected error"))))
