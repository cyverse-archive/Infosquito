(ns infosquito.progress
  (:require [clj-time.core :as ct]
            [clojure.tools.logging :as log])
  (:import [org.joda.time.format PeriodFormatterBuilder]))

(def ^:private period-formatter
  (-> (PeriodFormatterBuilder.)
      (.appendDays)
      (.appendSuffix " day", "days")
      (.appendSeparator ", ")
      (.appendHours)
      (.appendSuffix " hour", " hours")
      (.appendSeparator ", ")
      (.appendMinutes)
      (.appendSuffix " minute", " minutes")
      (.appendSeparator ", ")
      (.appendSeconds)
      (.appendSuffix " second", " seconds")
      (.toFormatter)))

(defn- create-notifier
  [notify-count f]
  (let [idx-start      (ref (ct/now))
        idx-count      (ref 0)
        get-interval   #(.print period-formatter (.toPeriod (ct/interval @idx-start (ct/now))))]
    (fn [entry]
      (let [r (f entry)
            c (dosync (alter idx-count inc))]
        (when (zero? (mod c notify-count))
          (log/info "over" c "processed in" (get-interval)))
        r))))

(defn notifier
  [notifications-enabled? notify-count f]
  (if notifications-enabled? (create-notifier notify-count f) f))
