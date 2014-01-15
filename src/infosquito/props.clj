(ns infosquito.props
  "This namespace holds all of the logic for managing the configuration values"
  (:require [clojure.tools.logging :as log])
  (:import [java.net URL]))


(def ^:private DEFAULT-AMQP-PORT        5672)
(def ^:private DEFAULT-INDEX-BATCH-SIZE 100)


(def ^:private prop-names
  ["infosquito.es.host"
   "infosquito.es.port"
   "infosquito.es.scroll-size"
   "infosquito.icat.host"
   "infosquito.icat.port"
   "infosquito.icat.user"
   "infosquito.icat.pass"
   "infosquito.icat.db"
   "infosquito.base-collection"
   "infosquito.index-batch-size"
   "infosquito.amqp.host"
   "infosquito.amqp.port"
   "infosquito.amqp.user"
   "infosquito.amqp.pass"
   "infosquito.amqp.reindex-queue"])

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
  (get props "infosquito.base-collection"))


(defn get-index-batch-size
  [props]
  (let [size-str (get props "infosquito.index-batch-size")]
    (try
      (Math/abs (Integer/parseInt size-str))
      (catch NumberFormatException e
        (log/fatal "invalid index batch size:" size-str)
        DEFAULT-INDEX-BATCH-SIZE))))

(defn get-amqp-host
  [props]
  (get props "infosquito.amqp.host"))


(defn get-amqp-port
  [props]
  (let [port-str (get props "infosquito.amqp.port")]
    (try
      (Integer/parseInt port-str)
      (catch NumberFormatException e
        (log/fatal "invalid AMQP port:" port-str)
        DEFAULT-AMQP-PORT))))


(defn get-amqp-user
  [props]
  (get props "infosquito.amqp.user"))


(defn get-amqp-pass
  [props]
  (get props "infosquito.amqp.pass"))


(defn get-amqp-reindex-queue
  [props]
  (get props "infosquito.amqp.reindex-queue"))


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
