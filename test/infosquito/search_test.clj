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
    (index "user2" "file")
    (index "user2" "efg")))

(defn- wait-for-index-completion 
  []
  (let [req (.. @client
              (prepareSearch (into-array ["data"]))
              (setQuery (QueryBuilders/wildcardQuery "name" "*")))]
    (loop []
      (if-not (<= 3 (.. req execute actionGet hits totalHits))
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

(deftest test-query-response
  (let [results (query @client "user1" "file" :score 0 10)
        match   (first results)]
    (is (= 1 (count results)))
    (is (= "/iplant/home/user1/file" (:path match)))
    (is (= "file" (:name match)))))

(deftest test-glob-query
  (let [results (query @client "user1" "f*" :score 0 10)]
    (is (= 1 (count results)))
    (is (= "file" (:name (first results))))))

(deftest test-sort-by-name
  (let [results (query @client "user2" "*" :name 0 10)]
    (is (= "efg" (:name (first results))))))

(deftest test-from
  (let [res0 (query @client "user2" "*" :score 0 10)
        res1 (query @client "user2" "*" :score 1 10)]
    (is (= (first res1) (second res0)))))

(deftest test-size
  (let [res (query @client "user2" "*" :score 0 1)]
    (is (= 1 (count res)))))
