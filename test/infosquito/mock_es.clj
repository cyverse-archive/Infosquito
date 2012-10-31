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
  [repo index params]
  (let [search-type (:search_type params)
        query       (:query params)]
    (when (not= search-type "scan")
      (ss/throw+ {:type :unsuported-search-type :search-type search-type}))
    (when (not= query (ceq/match-all))
      (ss/throw+ {:type :unsupported-query :query query}))
    (let [hits   (if-let [idx-map (get repo index)]
                   (map (fn [kv] {:_id (key kv)}) (apply merge (vals idx-map)))
                   [])
          scroll {:total-hits (count hits) :hits hits}]
      [scroll
       {:_scroll_id (str scroll)}])))

    
(defn- advance-scroll
  [scroll]
  (let [scroll' (assoc scroll :hits [])]
    [scroll' {:hits       {:hits (:hits scroll) :total (:total-hits scroll)}
              :_scroll_id (str scroll')}]))


(defrecord ^{:private true} MockIndexer [repo-ref scroll-ref]
  Indexes
  
  (delete [_ index type id]
    (swap! repo-ref remove-doc index type id)
    {:ok true})

  (exists? [_ index]
    (index-exists? @repo-ref index))
  
  (put [_ index type id doc-map]
    (swap! repo-ref index-doc index type id doc-map)
    {:ok true})

  (scroll [_ scroll-id keep-alive-time]
    (let [[scroll' resp] (advance-scroll @scroll-ref)]
      (reset! scroll-ref scroll')
      resp))
  
  (search-all-types [_ index params]
    (let [[scroll' resp] (search-index @repo-ref index params)] 
      (reset! scroll-ref scroll')
      resp)))


(defn mk-mock-indexer
  [repo-ref]
  (->MockIndexer repo-ref (atom {:hits [] :total-hits 0})))
