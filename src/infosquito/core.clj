(ns infosquito.core
  "This namespace defines the entry point for Infosquito.  All state should be
   in here."
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [com.github.drsnyder.beanstalk :as beanstalk]
            [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as irods]
            [clojure-commons.clavin-client :as zk]
            [clojure-commons.infosquito.work-queue :as queue]
            [clojure-commons.props :as props]
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


(defn- mk-queue
  [props]
  (let [port (get-int-prop props "infosquito.beanstalk.port")
        ctor #(beanstalk/new-beanstalk (get props "infosquito.beanstalk.host")
                                       port)]
    (queue/mk-client ctor
                     (get-int-prop props "infosquito.beanstalk.connect-retries")
                     (get-int-prop props "infosquito.beanstalk.task-ttr")
                     (get props "infosquito.beanstalk.tube"))))


(defn- mk-es-url
  [props]
  (str (URL. "http"
             (get props "infosquito.es.host")
             (get-int-prop props "infosquito.es.port")
             "")))


(defn- log-config
  [props]
  (dorun (map (fn [[k v]] (log/warn "CONFIG:" k "=" v)) (sort-by key props))))


(defn- run
  "Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :connection-refused - This is thrown if it can't connect to Elastic Search.
     :internal-error - This is thrown if there is an error in the logic error
       internal to the worker.
     :unknown-error - This is thrown if an unidentifiable error occurs.
     :beanstalkd-oom - This is thrown if beanstalkd is out of memory."
  [mode props]
  (log-config props)
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


(defn- get-local-props
  [cfg-file]
  (ss/try+
    (props/read-properties cfg-file)
    (catch Object _
      (ss/throw+ {:type :cfg-problem :cfg-file cfg-file}))))


(defn- get-remote-props
  []
  (let [zkprops (props/parse-properties "zkhosts.properties")]
    (zk/with-zk (get zkprops "zookeeper")
      (if (zk/can-run?)
        (zk/properties "infosquito")
        (ss/throw+ {:type :zk-perm})))))


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
    (catch [:type :cli] {:keys [msg]}
      (log/error (str "There was a problem reading the command line input. ("
                      msg ") Exiting.")))
    (catch [:type :invalid-mode] {:keys [mode]}
      (log/error "Invalid mode, " mode))
    (catch [:type :cfg-problem] {:keys [cfg-file]}
      (log/error str("There was problem reading the configuration file "
                      cfg-file ". Exiting.")))
    (catch [:type :zk-perm] {:keys []}
      (log/error "This application cannot run on this machine. Exiting."))
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
