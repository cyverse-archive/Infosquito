(ns infosquito.core
  "This namespace defines the entry point for Infosquito.  All state should be in here."
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [com.github.drsnyder.beanstalk :as beanstalk]
            [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as irods]
            [clojure-commons.config :as config]
            [clojure-commons.infosquito.work-queue :as queue]
            [infosquito.es :as es]
            [infosquito.irods-facade :as irods-wrapper]
            [infosquito.props :as props]
            [infosquito.worker :as worker])
  (:import [java.net URL]
           [java.util Properties]))


(defn- ->config-loader
  [& [cfg-file]]
  (if cfg-file
    (fn [props-ref] (config/load-config-from-file nil cfg-file props-ref))
    (fn [props-ref] (config/load-config-from-zookeeper props-ref "infosquito"))))


(defn- update-props
  [load-props props]
    (let [props-ref (ref props 
                         :validator (fn [p] (if (empty? p)
                                              true
                                              (props/validate p #(log/error %&)))))]
      (ss/try+
        (load-props props-ref)
        (catch Object _
          (log/error "Failed to load configuration parameters.")))
      (when (.isEmpty @props-ref)
        (ss/throw+ {:type :cfg-problem 
                    :msg  "Don't have any configuration parameters."}))
      (when-not (= props @props-ref) (config/log-config props-ref))
      @props-ref))
        

(defn- ->queue
  [props]
  (letfn [(ctor [] (beanstalk/new-beanstalk (props/get-beanstalk-host props) 
                                            (props/get-beanstalk-port props)))]
    (queue/mk-client ctor
                     1
                     (props/get-job-ttr props)
                     (props/get-work-tube props))))


(defn- ->worker
  [props queue irods]
  (worker/mk-worker queue
                    irods
                    (es/mk-indexer (str (props/get-es-url props)))
                    (props/get-irods-index-root props)
                    (props/get-es-scroll-ttl props)
                    (props/get-es-scroll-page-size props)))
 

(defn- init-irods
  [props]
  (irods/init (props/get-irods-host props)
              (str (props/get-irods-port props))
              (props/get-irods-user props)
              (props/get-irods-password props)
              (props/get-irods-home props)
              (props/get-irods-zone props)
              (props/get-irods-default-resource props)))


(defmacro ^{:private true} trap-exceptions!
  [& body]
  `(ss/try+
     (do ~@body)
     (catch [:type :connection-refused] {:keys [~'msg]}
       (log/error "connection failure." ~'msg))
     (catch [:type :connection] {:keys [~'msg]} 
       (log/error "connection failure." ~'msg))
     (catch [:type :beanstalkd-oom] {:keys []}
       (log/error "An error occurred. beanstalkd is out of memory and is"
                  " probably wedged."))
     (catch Object o# (log/error (:throwable ~'&throw-context) "unexpected error"))))


(defmacro ^{:private true} with-worker
  [props [worker-sym] & body]
  `(trap-exceptions!
     (let [queue#     (->queue ~props)
           irods-cfg# (init-irods ~props)]
       (queue/with-server queue# 
         (irods-wrapper/with-irods irods-cfg# [irods#]
           (let [~worker-sym (->worker ~props queue# irods#)]
             (do ~@body)))))))

  
(defn- process-jobs
  [load-props]
  (loop [old-props (Properties.)]
    (let [props (update-props load-props old-props)]
      (with-worker props [worker]
        (dorun (repeatedly #(worker/process-next-job worker))))
      (Thread/sleep (props/get-retry-delay props))
      (recur props))))


(defn- sync-index
  [load-props]
  (let [props (update-props load-props (Properties.))]
    (with-worker props [worker] 
      (worker/sync-index worker))))
  

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
      (log/error (str "There was a problem loading the configuration values. (" 
                      msg ") Exiting.")))
    (catch Object _
      (log/error (:throwable &throw-context) "unexpected error"))))
