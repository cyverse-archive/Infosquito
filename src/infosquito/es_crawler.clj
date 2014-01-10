(ns infosquito.es-crawler
  (:use [infosquito.progress :only [notifier]])
  (:require [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.document :as esd]
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

(defn- delete-item
  [item-type id]
  (log/trace "deleting index entry for" (name item-type) id)
  (esd/delete index (name item-type) id))

(defn- purge-deleted-items
  [item-type item-exists? props]
  (log/info "purging non-existent" (name item-type) "entries")
  (->> (item-seq item-type props)
       (map (notifier (cfg/notify-enabled? props) (cfg/get-notify-count props) :_id))
       (remove item-exists?)
       (map (partial delete-item item-type))
       (dorun))
  (log/info (name item-type) "entry purging complete"))

(def ^:private purge-deleted-files (partial purge-deleted-items :file icat/file-exists?))
(def ^:private purge-deleted-folders (partial purge-deleted-items :folder icat/folder-exists?))

(defn purge-index
  [props]
  (esr/connect! (cfg/get-es-url props))
  (purge-deleted-files props)
  (purge-deleted-folders props))
