(ns infosquito.es-crawler
  (:use [infosquito.progress :only [notifier]])
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.bulk :as bulk]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.response :as resp]
            [clojurewerkz.elastisch.query :as q]
            [infosquito.icat :as icat]
            [infosquito.props :as cfg]))

(def ^:private index "data")

(defn- item-seq
  [item-type props]
  (esd/scroll-seq (esd/search index (name item-type)
                              :query       (q/match-all)
                              :fields      ["_id"]
                              :search_type "query_then_fetch"
                              :scroll      "1m"
                              :size        (cfg/get-es-scroll-size props))))

(defn- log-deletion
  [item-type item]
  (log/trace "deleting index entry for" (name item-type) (:_id item)))

(defn- log-failure
  [item-type item]
  (log/trace "unable to remove the index entry for" (name item-type) (:_id item)))

(defn- log-failures
  [res]
  (->> (:items res)
       (map :delete)
       (filter (complement resp/ok?))
       (map (fn [{id :_id type :_type}] (log-failure type id)))
       (dorun)))

(defn- delete-items
  [item-type items]
  (dorun (map (partial log-deletion item-type) items))
  (try
    (let [req (bulk/bulk-delete items)
          res (bulk/bulk-with-index-and-type index (:name item-type) req :refresh true)]
      (log-failures res))
    (catch Throwable t
      (dorun (map (partial log-failure (name item-type)) (map :id items))))))

(defn- existence-logger
  [item-type item-exists?]
  (fn [id]
    (let [exists? (item-exists? id)]
      (log/trace (name item-type) id (if exists? "exists" "does not exist"))
      exists?)))

(defn- purge-deleted-items
  [item-type item-exists? props]
  (log/info "purging non-existent" (name item-type) "entries")
  (->> (item-seq item-type props)
       (mapcat (comp (notifier (cfg/notify-enabled? props) (cfg/get-notify-count props)) vector))
       (remove (comp (existence-logger item-type item-exists?) :_id))
       (partition-all (cfg/get-index-batch-size props))
       (map (partial delete-items item-type))
       (dorun))
  (log/info (name item-type) "entry purging complete"))

(def ^:private purge-deleted-files (partial purge-deleted-items :file icat/file-exists?))
(def ^:private purge-deleted-folders (partial purge-deleted-items :folder icat/folder-exists?))

(defn purge-index
  [props]
  (esr/connect! (cfg/get-es-url props))
  (purge-deleted-files props)
  (purge-deleted-folders props))
