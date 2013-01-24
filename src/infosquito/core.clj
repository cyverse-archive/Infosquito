(ns infosquito.core
  "All state should be in here."
  (:require [clojure.tools.logging :as log]
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


(defn run-local
  [mode cfg-file]
  ((->mode mode) #(config/load-config-from-file nil cfg-file %)))


(defn run-zookeeper
  [mode]
  ((->mode mode) #(config/load-config-from-zookeeper % "infosquito")))
