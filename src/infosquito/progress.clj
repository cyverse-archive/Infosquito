(ns infosquito.progress
  (:require [clj-time.core :as ct])
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

(defn notifier
  [notify-step f]
  (let [idx-start    (ref (ct/now))
        idx-count    (ref 0)
        notify-count (ref notify-step)
        get-interval #(.print period-formatter (.toPeriod (ct/interval @idx-start (ct/now))))]
    (fn [entries]
      (let [r (f entries)]
        (dosync
          (let [c (alter idx-count (partial + (count entries)))]
            (when (>= c @notify-count)
              (println "Over" @notify-count "processed in" (get-interval))
              (alter notify-count (partial + notify-step)))))
        r))))
