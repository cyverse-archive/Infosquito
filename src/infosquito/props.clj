(ns infosquito.props
  "This namespace holds all of the logic for managing the configuration values"
  (:import [java.net URL]))


(def ^:private prop-names
  ["infosquito.es.host"
   "infosquito.es.port"
   "infosquito.icat.host"
   "infosquito.icat.port"
   "infosquito.icat.user"
   "infosquito.icat.pass"
   "infosquito.icat.db"
   "infosquito.base.collection"])

(defn get-es-host
  [props]
  (get props "infosquito.es.host"))


(defn get-es-port
  [props]
  (get props "infosquito.es.port"))


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
