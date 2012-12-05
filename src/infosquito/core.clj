(ns infosquito.core
  "This namespace defines the entry point for Infosquito.  All state should be
   in here."
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [com.github.drsnyder.beanstalk :as beanstalk]
            [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as irods]
            [clojure-commons.config :as config]
            [clojure-commons.infosquito.work-queue :as queue]
            [infosquito.es :as es]
            [infosquito.worker :as worker])
  (:import [java.net URL]))


(defn- get-int-prop
  [props name]
  (Integer/parseInt (get props name)))


(defn- init-irods
  [props]
  (irods/init (get props "infosquito.irods.host")
              (get props "infosquito.irods.port")
              (get props "infosquito.irods.user")
              (get props "infosquito.irods.password")
              (get props "infosquito.irods.home")
              (get props "infosquito.irods.zone")
              (get props "infosquito.irods.default-resource")))


(defn- mk-es-url
  [props]
  (str (URL. "http"
             (get props "infosquito.es.host")
             (get-int-prop props "infosquito.es.port")
             "")))


(defn- mk-queue
  [props]
  (let [port (get-int-prop props "infosquito.beanstalk.port")
        ctor #(beanstalk/new-beanstalk (get props "infosquito.beanstalk.host")
                                       port)]
    (queue/mk-client ctor
                     (get-int-prop props "infosquito.beanstalk.connect-retries")
                     (get-int-prop props "infosquito.beanstalk.task-ttr")
                     (get props "infosquito.beanstalk.tube"))))


(defn- load-props
  [cfg-file]
  (ss/try+
    (let [props (ref nil)]
      (if cfg-file
        (config/load-config-from-file nil cfg-file props)
        (config/load-config-from-zookeeper props "infosquito"))
      @props)
    (catch Object _
      (ss/throw+ {:type :cfg-problem}))))
        
  
(defn- map-mode
  [mode-str]
  (condp = mode-str
    "sync"   :sync
    "worker" :worker
             (ss/throw+ {:type :invalid-mode :mode mode-str})))


(defn- parse-args
  [args]
  (ss/try+
    (cli/cli args
      ["-c" "--config"
       "sets the local configuration file to be read, bypassing Zookeeper"]
      ["-h" "--help"
       "show help and exit"
       :flag true]
      ["-m" "--mode"
       "Indicates how infosquito should be run. (sync|worker)"
       :default "worker"])
    (catch Exception e
      (ss/throw+ {:type :cli :msg (.getMessage e)}))))


(defn- run
  "Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :connection-refused - This is thrown if it can't connect to Elastic Search.
     :internal-error - This is thrown if there is an error in the logic error
       internal to the worker.
     :unknown-error - This is thrown if an unidentifiable error occurs.
     :beanstalkd-oom - This is thrown if beanstalkd is out of memory."
  [mode props]
  (config/log-config props)
  (let [worker (worker/mk-worker (init-irods props)
                                 (mk-queue props)
                                 (es/mk-indexer (mk-es-url props))
                                 (get props "infosquito.irods.index-root")
                                 (get props "infosquito.es.scroll-ttl")
                                 (get-int-prop props
                                               "infosquito.es.scroll-page-size"))]
    (condp = mode
      :sync   (worker/sync-index worker)))
      :worker (dorun (repeatedly #(worker/process-next-task worker))))


(defn -main
  [& args]
  (ss/try+
    (let [[opts _ help-str] (parse-args args)]
      (if (:help opts)
        (println help-str)
        (run (map-mode (:mode opts)) (load-props (:config opts)))))
    (catch [:type :cli] {:keys [msg]}
      (log/error (str "There was a problem reading the command line input. ("
                      msg ") Exiting.")))
    (catch [:type :invalid-mode] {:keys [mode]}
      (log/error "Invalid mode, " mode))
    (catch [:type :cfg-problem] {:keys []}
      (log/error "There was problem loading the configuration values.  Exiting."))
    (catch [:type :connection-refused] {:keys [msg]}
      (log/error (str "Cannot connect to Elastic Search. (" msg ") Exiting.")))
    (catch [:type :connection] {:keys [msg]}
      (log/error (str "An error occurred while communicating with the work queue. "
                      "(" msg ") Exiting.")))
    (catch [:type :beanstalkd-oom] {:keys []}
      (log/error "An error occurred. beanstalkd is out of memory and is"
                 "probably wedged. Exiting."))
    (catch Object _
      (log/error (:throwable &throw-context) "unexpected error"))))
