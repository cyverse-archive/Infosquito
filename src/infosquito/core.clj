(ns infosquito.core
  "This namespace defines the entry point for Infosquito. All state should be in here."
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [com.github.drsnyder.beanstalk :as beanstalk]
            [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as irods]
            [clojure-commons.config :as config]
            [clojure-commons.infosquito.work-queue :as queue]
            [infosquito.es :as es]
            [infosquito.exceptions :as exn]
            [infosquito.irods-facade :as irods-wrapper]
            [infosquito.irods-utils :as irods-utils]
            [infosquito.props :as props]
            [infosquito.worker :as worker])
  (:import [java.util Properties]))


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
                    (props/get-job-ttr props)
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
     (catch [:type :connection-refused] {:keys [~'msg]} (log/error "connection failure." ~'msg))
     (catch [:type :connection] {:keys [~'msg]} (log/error "connection failure." ~'msg))
     (catch [:type :beanstalkd-oom] {:keys []}
       (log/error "An error occurred. beanstalkd is out of memory and is probably wedged."))
     (catch Object o# (log/error (exn/fmt-throw-context ~'&throw-context)))))


(defmacro ^{:private true} with-worker
  [props [worker-sym] & body]
  `(trap-exceptions!
     (let [queue#     (->queue ~props)
           irods-cfg# (init-irods ~props)]
       (queue/with-server queue#
         (irods-wrapper/with-irods irods-cfg# [irods#]
           (irods-utils/define-specific-queries irods#)
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


(defn- run-local
  [cfg-file]
  (process-jobs #(config/load-config-from-file nil cfg-file %)))


(defn- run-zookeeper
  []
  (process-jobs #(config/load-config-from-zookeeper % "infosquito")))


(defn- parse-args
  [args]
  (cli/cli args
    ["-c" "--config" "sets the local configuration file to be read, bypassing Zookeeper"]
    ["-h" "--help" "show help and exit"
          :flag true]))


(defn -main
  [& args]
  (ss/try+
    (let [[opts _ help-str] (parse-args args)]
      (when (:help opts)
        (println help-str)
        (System/exit 0))
      (if-let [config (:config opts)]
        (run-local config)
        (run-zookeeper)))
    (catch Object _ (log/error (:throwable &throw-context) "UNEXPECTED ERROR - EXITING"))))
