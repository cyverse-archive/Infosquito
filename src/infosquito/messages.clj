(ns infosquito.messages
  (:require [clojure.tools.logging :as log]
            [infosquito.actions :as actions]
            [infosquito.props :as cfg]
            [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]))

(def ^:const exchange "amq.direct")

(defn- amqp-connect
  [props]
  (rmq/connect {:host     (cfg/get-amqp-host props)
                :port     (cfg/get-amqp-port props)
                :username (cfg/get-amqp-user props)
                :password (cfg/get-amqp-pass props)}))

(defn- declare-queue
  [ch queue-name]
  (lq/declare ch queue-name
              :durable     true
              :auto-delete false
              :exclusive   false)
  (lq/bind ch queue-name exchange :routing-key queue-name))

(defn- reindex-handler
  [props ch {:keys [delivery-tag]} _]
  (actions/reindex props)
  (lb/ack ch delivery-tag))

(defn- add-reindex-subscription
 [props ch]
 (let [queue-name (cfg/get-amqp-reindex-queue props)]
   (declare-queue ch queue-name)
   (lc/blocking-subscribe ch queue-name (partial reindex-handler props))))

(defn subscribe
  "Subscribes to incoming messages from AMQP."
  [props]
  (let [conn (amqp-connect props)
        ch   (lch/open conn)]
    (try
      (add-reindex-subscription props ch)
      (catch Exception e (log/error e "error occurred during message processing"))
      (finally
        (rmq/close ch)
        (rmq/close conn)))))
