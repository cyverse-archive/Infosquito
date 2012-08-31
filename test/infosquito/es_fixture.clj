(ns infosquito.es-fixture
  (:require [clojure.data.json :as dj])
  (:import [org.elasticsearch.common.settings ImmutableSettings]
           [org.elasticsearch.index.query QueryBuilders]
           [org.elasticsearch.node NodeBuilder]))

(defn- populate-indices
  [client]
  (letfn [(index ([user file] (.. client
                                (prepareIndex "data" 
                                              "file" 
                                              (str "/iplant/home/" user "/" file))
                                (setSource (dj/json-str {:user user :name file}))
                                execute
                                actionGet)))]
    (index "user1" "file")
    (index "user2" "file")
    (index "user2" "efg")))

(defn- wait-for-index-completion 
  [client]
  (let [req (.. client
              (prepareSearch (into-array ["data"]))
              (setQuery (QueryBuilders/wildcardQuery "name" "*")))]
    (loop []
      (if-not (<= 3 (.. req execute actionGet hits totalHits))
        (do 
          (Thread/sleep 1)
          (recur))))))

(defn mk-local-cluster
  [client-ref]
  (fn [test-fn]
  (let [settingsBldr (.. ImmutableSettings 
                       settingsBuilder
                       (put "path.data" "es-test-fixture")
                       (put "gateway.type" "none")
                       (put "index.store.type" "memory"))
        node         (.. NodeBuilder
                       nodeBuilder
                       (local true)
                       (settings settingsBldr)
                       node)]
    (try
      (reset! client-ref (.client node))
      (populate-indices @client-ref)
      (wait-for-index-completion @client-ref)
      (test-fn)
      (finally
        (reset! client-ref nil)
        (.close node))))))
