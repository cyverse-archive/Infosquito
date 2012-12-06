(ns infosquito.core
  "This namespace defines the entry point for Infosquito.  All state should be
   in here."
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [com.github.drsnyder.beanstalk :as beanstalk]
            [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as irods]
            [clojure-commons.config :as cc-config]
            [clojure-commons.infosquito.work-queue :as queue]
            [infosquito.config :as config]
            [infosquito.es :as es]
            [infosquito.worker :as worker])
  (:import [java.net URL]
           [java.util Properties]))


(defn- ->config-loader
  [& [cfg-file]]
  (if cfg-file
    (fn [props-ref] (cc-config/load-config-from-file nil cfg-file props-ref))
    (fn [props-ref] (cc-config/load-config-from-zookeeper props-ref "infosquito"))))


(defn- init-irods
  [props]
  (irods/init (config/get-irods-host props)
              (str (config/get-irods-port props))
              (config/get-irods-user props)
              (config/get-irods-password props)
              (config/get-irods-home props)
              (config/get-irods-zone props)
              (config/get-irods-default-resource props)))


(defn- mk-queue
  [props]
  (letfn [(ctor [] (beanstalk/new-beanstalk (get-beanstalk-port props) 
                                            (get-beanstalk-port props)))]
    (queue/mk-client ctor
                     (get-beanstalk-connect-retries props)
                     (get-beanstalk-job-ttr props)
                     (get-beanstalk-tube props))))


(defn- mk-worker
  [props]
  (worker/mk-worker (init-irods props)
                    (mk-queue props)
                    (es/mk-indexer (str (config/get-es-url props)))
                    (get-irods-index-root props)
                    (get-es-scroll-ttl props)
                    (get-es-scroll-page-size props)))
 

(defn- update-props
  [load-props props]
    (let [props-ref (ref props :validator #(config/validate-props % log/error))]
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
