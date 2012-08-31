(ns infosquito.search-test
  (:use clojure.test
        infosquito.search)
  (:require [clojure.data.json :as dj])
  (:import [org.elasticsearch.common.settings ImmutableSettings]
           [org.elasticsearch.index.query QueryBuilders]
           [org.elasticsearch.node NodeBuilder]))

(def ^{:private true} client (atom nil))

(defn- populate-indices
  []
  (letfn [(index ([user file] (.. @client
                                (prepareIndex "data" 
                                              "file" 
                                              (str "/iplant/home/" user "/" file))
                                (setSource (dj/json-str {:user user :name file}))
                                execute
                                actionGet)))]
    (index "user1" "file")
    (index "user2" "file")))

(defn- wait-for-index-completion 
  []
  (let [req (.. @client
              (prepareSearch (into-array ["data"]))
              (setQuery (QueryBuilders/termQuery "name" "file")))]
    (loop []
      (if-not (<= 2 (.. req execute actionGet hits totalHits))
        (do 
          (Thread/sleep 1)
          (recur))))))

(defn- with-local-cluster
  [test-fn]
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
      (reset! client (.client node))
      (populate-indices)
      (wait-for-index-completion)
      (test-fn)
      (finally
        (.close node)
        (reset! client nil)))))
      
(use-fixtures :once with-local-cluster)

(deftest test-query-filter
  (let [results (query @client "user1" "file" 0 10 :score)]
    (is (= 1 (count results)))
    (is (= "/iplant/home/user1/file" (first results)))))
