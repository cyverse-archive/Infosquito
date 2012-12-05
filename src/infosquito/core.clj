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
  (:import [java.net URL]
           [java.util Properties]))


(defn- ->config-loader
  [& [cfg-file]]
  (if cfg-file
    (fn [props-ref] (config/load-config-from-file nil cfg-file props-ref))
    (fn [props-ref] (config/load-config-from-zookeeper props-ref "infosquito"))))


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


(defn- mk-worker
  [props]
  (worker/mk-worker (init-irods props)
                    (mk-queue props)
                    (es/mk-indexer (mk-es-url props))
                    (get props "infosquito.irods.index-root")
                    (get props "infosquito.es.scroll-ttl")
                    (get-int-prop props "infosquito.es.scroll-page-size")))
 

(defn- validate-props
  [props]
  (let [validate    (fn [label] (when-not (get props label) 
                                  (log/error "The property" label 
                                             "is missing from the configuration.")
                                  false))
        prop-labels ["infosquito.beanstalk.host"
                     "infosquito.beanstalk.port"
                     "infosquito.beanstalk.connect-retries"
                     "infosquito.beanstalk.task-ttr"
                     "infosquito.beanstalk.tube"
                     "infosquito.es.host"
                     "infosquito.es.port"
                     "infosquito.es.scroll-ttl"
                     "infosquito.es.scroll-page-size"
                     "infosquito.irods.host"
                     "infosquito.irods.port"
                     "infosquito.irods.user"
                     "infosquito.irods.password"
                     "infosquito.irods.home"
                     "infosquito.irods.zone"
                     "infosquito.irods.default-resource"
                     "infosquito.irods.index-root"
                     "infosquito.retry-delay"]]
    (not-any? #(false? (validate %)) prop-labels)))
    
  
(defn- update-props
  [load-props props]
    (let [props-ref (ref props :validator validate-props)]
      (ss/try+
        (load-props props-ref)
        (catch Object _
          (log/error "Failed to load configuration parameters.")))
      (when (.isEmpty @props-ref)
        (ss/throw+ {:type :cfg-problem 
                    :msg "Don't have any configuration parameters."}))
      (when-not (= props @props-ref) (config/log-config @props-ref))
      @props-ref))
        

(defn- process-jobs
  [load-props]
  (loop [old-props (Properties.)]
    (let [props (update-props load-props old-props)]
      (ss/try+
        (dorun (repeatedly #(worker/process-next-task (mk-worker props))))
        (catch [:type :connection-refused] {:keys [msg]}
          (log/error "Cannot connect to Elastic Search." msg))
        (catch [:type :connection] {:keys [msg]}
          (log/error "An error occurred while communicating with Beanstalk." msg))
        (catch [:type :beanstalkd-oom] {:keys []}
          (log/error "An error occurred. beanstalkd is out of memory and is"
                     "probably wedged.")))
      (.wait props (get-int-prop "infosquito.retry-delay"))
      (recur props))))


(defn- sync-index
  [load-props]
  (let [worker (mk-worker (update-props load-props (Properties.)))]
    (worker/sync-index worker)))
  

(defn- ->mode
  [mode-str]
  (condp = mode-str
    "sync"   sync-index
    "worker" process-jobs
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
        ((->mode (:mode opts)) (->config-loader (:config opts)))))
    (catch [:type :cli] {:keys [msg]}
      (log/error (str "There was a problem reading the command line input. ("
                      msg ") Exiting.")))
    (catch [:type :invalid-mode] {:keys [mode]}
      (log/error "Invalid mode, " mode))
    (catch [:type :cfg-problem] {:keys [msg]}
      (log/error (str "There was problem loading the configuration values. (" 
                      msg ") Exiting.")))
    (catch Object _
      (log/error (:throwable &throw-context) "unexpected error"))))
