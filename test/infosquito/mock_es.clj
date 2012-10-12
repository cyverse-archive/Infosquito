(ns infosquito.mock-es
  "The internal representation will be as follows.
   
   {\"index-1\" {\"type-1\" {\"id-1\" doc-1-map
                             \"id-2\" doc-2-map
                             ...}
                 \"type-2\" ...
                 ...}
    \"index-2\" ...
    ...}"
  (:use infosquito.es-if)
  (:require [clojurewerkz.elastisch.query :as ceq]
            [slingshot.slingshot :as ss]))


(defn- index-doc
  [repo index type id doc]
  (assoc-in repo [index type id] doc))


(defn- index-exists?
  [repo index]
  (contains? repo index))

    
(defn- remove-doc
  [repo index type id]
  (let [idx-map (get repo index)]
    (if-let [type-map (get idx-map type)]
      (->> id (dissoc type-map) (assoc idx-map type) (assoc repo index))
      repo)))
  

(defn- search-index
  [repo index query]
  (if (not= query (ceq/match-all))
    (ss/throw+ {:type :unsupported-query :query query})
    (when-let [idx-map (get repo index)]
      (let [hits (map (fn [kv] {:_id (key kv)}) (apply merge (vals idx-map)))]
        {:hits {:hits hits :total (count hits)}}))))
    
  
(defrecord MockIndexer [repo-ref]
  Indexes
  
  (delete [_ index type id]
    (swap! repo-ref remove-doc index type id))

  (exists? [_ index]
    (index-exists? @repo-ref index))
  
  (put [_ index type id doc-map]
    (swap! repo-ref index-doc index type id doc-map))
  
  (search-all-types [_ index query]
    (search-index @repo-ref index query)))
