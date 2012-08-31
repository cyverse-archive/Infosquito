(ns infosquito.search
  (:import [org.elasticsearch.client Client]
           [org.elasticsearch.action.search SearchType]
           [org.elasticsearch.index.query FilterBuilders QueryBuilders]
           [org.elasticsearch.node NodeBuilder]
           [org.elasticsearch.search.sort SortOrder]))

(defn with-elasticsearch
  [cluster service]
  (let [node (.. NodeBuilder 
               (nodeBuilder) 
               (clusterName cluster)
               (client true)
               (node))]
    (try
      (service (.client node))
      (finally
        (.close node)))))

; TODO handle failure
(defn query
  [client user name sort-by window]
  (let [query (QueryBuilders/filteredQuery
                (QueryBuilders/wildcardQuery "name" name)
                (FilterBuilders/termFilter "user" user))
        req   (.. client
                (prepareSearch (into-array ["data"]))
                (setSearchType SearchType/DFS_QUERY_THEN_FETCH)
                (setQuery query)
                (setFrom (window 0))
                (setSize (- (window 1) (window 0))))
        resp  (.. (if (= sort-by :score)
                    req
                    (.addSort req "name" SortOrder/ASC))
                execute
                actionGet)
        hits  (.. resp hits hits)]
    (for [idx (range (count hits))]
      (let [hit (aget hits idx)]
        {:index (+ idx (window 0))
         :path  (.id hit)
         :name  (get (.getSource hit) "name")}))))
  