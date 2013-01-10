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


(defn- ensure-not-fail
  [state]
  (when (:fail? state) (ss/throw+ {:type :forced-fail})))
  
  
(defn- index-doc
  [state index type id doc]
  (assoc-in state [:repo index type id] doc))


(defn- index-exists?
  [state index]
  (contains? (:repo state) index))

    
(defn- remove-doc
  [state index type id]
  (if-let [type-map (get-in state [:repo index type])]
    (assoc-in state [:repo index type] (dissoc type-map id))
    state))
  

(defn- search-index
  [state index params]
  (let [search-type (:search_type params)
        query       (:query params)]
    (when (not= search-type "scan")
      (ss/throw+ {:type :unsuported-search-type :search-type search-type}))
    (when (not= query (ceq/match-all))
      (ss/throw+ {:type :unsupported-query :query query}))
    (let [hits   (if-let [idx-map (get-in state [:repo index])]
                   (map (fn [kv] {:_id (key kv)}) (apply merge (vals idx-map)))
                   [])
          scroll {:total-hits (count hits) :hits hits}]
      [(assoc state :scroll scroll)
       {:_scroll_id (str scroll)}])))

    
(defn- advance-scroll
  [state]
  (let [scroll  (:scroll state)
        scroll' (assoc scroll :hits [])]
    [(assoc state :scroll scroll') 
     {:hits       {:hits (:hits scroll) :total (:total-hits scroll)}
      :_scroll_id (str scroll')}]))


(defn mk-indexer-state
  []
  {:fail?  false
   :repo   {}
   :scroll {:hits [] :total-hits 0}})

 
(defn set-contents
  [state contents]
  (assoc state :repo contents))


(defn get-doc
  [state index type id]
  (get-in state [:repo index type id]))

    
(defn has-index?
  [state index]
  (boolean (get-in state [:repo index])))

  
(defn indexed? 
  [state index type id]
  (boolean (get-doc state index type id)))


(defn fail-ops
  [state fail?]
  (assoc state :fail? fail?))

  
(defrecord MockIndexer [state-ref]
  Indexes
  
  (delete [_ index type id]
    (ensure-not-fail @state-ref)
    (swap! state-ref remove-doc index type id)
    {:ok true})

  (exists? [_ index]
    (ensure-not-fail @state-ref)
    (index-exists? @state-ref index))
  
  (put [_ index type id doc-map]
    (ensure-not-fail @state-ref)
    (swap! state-ref index-doc index type id doc-map)
    {:ok true})

  (scroll [_ scroll-id keep-alive-time]
    (ensure-not-fail @state-ref)
    (let [[state' resp] (advance-scroll @state-ref)]
      (reset! state-ref state')
      resp))
  
  (search-all-types [_ index params]
    (ensure-not-fail @state-ref)
    (let [[state' resp] (search-index @state-ref index params)] 
      (reset! state-ref state')
      resp)))
