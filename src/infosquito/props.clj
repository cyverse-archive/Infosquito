(ns infosquito.props
  "This namespace holds all of the logic for managing the configuration values"
  (:import [java.net URL]))


(defn- get-int-prop
  [props name]
  (Integer/parseInt (get props name)))


(defn- get-boolean-prop
  [props name]
  (Boolean/parseBoolean (get props name)))


(defn get-amqp-host
  [props]
  (get props "infosquito.amqp.host"))


(defn get-amqp-port
  [props]
  (get-int-prop props "infosquito.amqp.port"))


(defn get-amqp-user
  [props]
  (get props "infosquito.amqp.user"))


(defn get-amqp-pass
  [props]
  (get props "infosquito.amqp.pass"))


(defn get-amqp-exchange
  [props]
  (get props "infosquito.amqp.exchange"))


(defn get-amqp-exchange-type
  [props]
  (get props "infosquito.amqp.exchange.type"))


(defn amqp-exchange-durable?
  [props]
  (get-boolean-prop props "infosquito.amqp.exchange.durable"))


(defn amqp-exchange-auto-delete?
  [props]
  (get-boolean-prop props "infosquito.amqp.exchange.auto-delete"))


(defn amqp-msg-auto-ack?
  [props]
  (get-boolean-prop props "infosquito.amqp.msg.auto-ack"))


(defn get-amqp-health-check-interval
  [props]
  (get-int-prop props "infosquito.amqp.health-check-interval"))


(defn get-amqp-routing-key
  [props]
  (get props "infosquito.amqp.queue.routing-key"))


(defn get-es-scroll-page-size
  [props]
  (get-int-prop props "infosquito.es.scroll-page-size"))


(defn get-es-scroll-ttl
  [props]
  (get props "infosquito.es.scroll-ttl"))


(defn get-es-url
  [props]
  (URL. "http"
        (get props "infosquito.es.host")
        (get-int-prop props "infosquito.es.port")
        ""))


(defn get-irods-default-resource
  [props]
  (get props "infosquito.irods.default-resource"))


(defn get-irods-home
  [props]
  (get props "infosquito.irods.home"))


(defn get-irods-host
  [props]
  (get props "infosquito.irods.host"))


(defn get-irods-index-root
  [props]
  (get props "infosquito.irods.index-root"))


(defn get-irods-password
  [props]
  (get props "infosquito.irods.password"))


(defn get-irods-port
  [props]
  (get-int-prop props "infosquito.irods.port"))


(defn get-irods-user
  [props]
  (get props "infosquito.irods.user"))


(defn get-irods-zone
  [props]
  (get props "infosquito.irods.zone"))


(defn get-retry-delay
  [props]
  (get-int-prop props "infosquito.retry-delay"))


(defn validate
  [props log-invalid]
  (let [exists? (fn [label] (if (get props label)
                              true
                              (do
                                (log-invalid "The property" label
                                             "is missing from the configuration.")
                                false)))
        labels ["infosquito.es.host"
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
    (every? exists? labels)))
