(ns infosquito.search-test
  (:use clojure.test
        infosquito.es-fixture
        infosquito.search))

(def ^{:private true} es-client (atom nil))

(use-fixtures :once (mk-local-cluster es-client))

(deftest test-query-response
  (let [results (query @es-client "user1" "file" :score [0 10])
        match   (first results)]
    (is (= 1 (count results)))
    (is (= 0 (:index match)))
    (is (= "/iplant/home/user1/file" (:path match)))
    (is (= "file" (:name match)))))

(deftest test-glob-query
  (let [results (query @es-client "user1" "f*" :score [0 10])]
    (is (= 1 (count results)))
    (is (= "file" (:name (first results))))))

(deftest test-sort-by-name
  (let [results (query @es-client "user2" "*" :name [0 10])]
    (is (= "efg" (:name (first results))))))

(deftest test-from
  (let [res0 (query @es-client "user2" "*" :score [0 10])
        res1 (query @es-client "user2" "*" :score [1 11])]
    (is (= (first res1) (second res0)))))

(deftest test-size
  (let [res (query @es-client "user2" "*" :score [0 1])]
    (is (= 1 (count res)))))
