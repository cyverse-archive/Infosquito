(ns infosquito.es
  (:use infosquito.es-if)
  (:require [clojurewerkz.elastisch.rest :as cer]
            [clojurewerkz.elastisch.rest.document :as cerd]
            [clojurewerkz.elastisch.rest.index :as ceri]))


(defrecord ^{:private true} Indexer []
  Indexes
  
  (delete [_ index type id]
    (cerd/delete index type id))
  
  (exists? [_ index])
  
  (put [_ index type id doc-map]
    (cerd/put index type id doc-map))
  
  (search-all-types [_ index query]
    (cerd/search-all-types index :query query)))


(defn mk-indexer
  [es-url]
  (cer/connect! es-url)
  (->Indexer))