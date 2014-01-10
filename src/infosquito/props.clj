(ns infosquito.props
  "This namespace holds all of the logic for managing the configuration values"
  (:require [clojure.tools.logging :as log])
  (:import [java.net URL]))


(def ^:private prop-names
  ["infosquito.es.host"
   "infosquito.es.port"
   "infosquito.es.scroll-size"
   "infosquito.icat.host"
   "infosquito.icat.port"
   "infosquito.icat.user"
   "infosquito.icat.pass"
   "infosquito.icat.db"
   "infosquito.base.collection"
   "infosquito.amqp.host"
   "infosquito.amqp.port"
   "infosquito.amqp.user"
   "infosquito.amqp.pass"
   "infosquito.amqp.reindex-queue"
   "infosquito.notify.enabled"
   "infosquito.notify.count"])


(defn- get-int
  [props prop-name description]
  (let [string-value (get props prop-name)]
    (try
      (Integer/parseInt string-value)
      (catch NumberFormatException e
        (log/fatal "invalid" description "-" string-value)
        (System/exit 1)))))


(defn get-es-host
  [props]
  (get props "infosquito.es.host"))


(defn get-es-port
  [props]
  (get props "infosquito.es.port"))


(defn get-es-url
  [props]
  (str "http://" (get-es-host props) ":" (get-es-port props)))


(defn get-es-scroll-size
  [props]
  (get props "infosquito.es.scroll-size"))


(defn get-icat-host
  [props]
  (get props "infosquito.icat.host"))


(defn get-icat-port
  [props]
  (get props "infosquito.icat.port"))


(defn get-icat-user
  [props]
  (get props "infosquito.icat.user"))


(defn get-icat-pass
  [props]
  (get props "infosquito.icat.pass"))


(defn get-icat-db
  [props]
  (get props "infosquito.icat.db"))


(defn get-base-collection
  [props]
  (get props "infosquito.base.collection"))


(defn get-amqp-host
  [props]
  (get props "infosquito.amqp.host"))


(defn get-amqp-port
  [props]
  (get-int props "infosquito.amqp.port" "AMQP port"))


(defn get-amqp-user
  [props]
  (get props "infosquito.amqp.user"))


(defn get-amqp-pass
  [props]
  (get props "infosquito.amqp.pass"))


(defn get-amqp-reindex-queue
  [props]
  (get props "infosquito.amqp.reindex-queue"))


(defn notify-enabled?
  [props]
  (Boolean/parseBoolean (get props "infosquito.notify.enabled")))


(defn get-notify-count
  [props]
  (get-int props "infosquito.notify.count" "notify count"))


(defn- prop-exists?
  [props log-invalid prop-name]
  (or (get props prop-name)
      (do (log-invalid prop-name) false)))


(defn validate
  "Validates the configuration. We don't want short-circuit evaluation in this case because
   logging all missing configuration settings is helpful."
  [props log-invalid]
  (reduce (fn [a b] (and a b))
          (map (partial prop-exists? props log-invalid) prop-names)))
