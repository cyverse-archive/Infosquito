(ns infosquito.search
  (:import [org.elasticsearch.client Client]
           [org.elasticsearch.action.search SearchType]
           [org.elasticsearch.index.query FilterBuilders QueryBuilders]
           [org.elasticsearch.node NodeBuilder]))

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
  [client user name from count sort-by]
  (let [query (QueryBuilders/filteredQuery
                (QueryBuilders/termQuery "name" name)
                (FilterBuilders/termFilter "user" user))
        resp  (.. client
                (prepareSearch (into-array ["data"]))
                (setSearchType SearchType/DFS_QUERY_THEN_FETCH)
                (setQuery query)
                (setFrom from)
                (setSize count)
                execute
                actionGet)]
    (map #(.id %) (.. resp hits hits))))
  