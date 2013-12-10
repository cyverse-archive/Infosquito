(ns infosquito.props-test
  (:use [clojure.java.io :only [reader]]
        [clojure.test]
        [infosquito.props])
  (:import [java.util Properties]))


(def ^:private props
  (doto (Properties.)
    (.load (reader "dev-resources/local.properties"))))


(def ^:private bad-props
  (doto (Properties.)
    (.load (reader "dev-resources/empty.properties"))))


(deftest test-get-es-host
  (is (= "elastic-host" (get-es-host props))))

(deftest test-get-es-port
  (is (= "31338" (get-es-port props))))

(deftest test-get-icat-host
  (is (= "icat-host" (get-icat-host props))))

(deftest test-get-icat-port
  (is (= "5432" (get-icat-port props))))

(deftest test-get-icat-user
  (is (= "icat-user" (get-icat-user props))))

(deftest test-get-icat-pass
  (is (= "icat-pass" (get-icat-pass props))))

(deftest test-get-icat-db
  (is (= "icat-db" (get-icat-db props))))

(deftest test-get-base-collection
  (is (= "/iplant/home" (get-base-collection props))))

(deftest test-validate
  (testing "all properties to be validated are in local.properites"
    (is (validate props println)))
  (testing "all properties in local.properties are validated"
    (let [ks        (set (keys props))
          missing   (atom #{})
          log-error (fn [k] (swap! missing conj k))]
      (is (false? (validate bad-props log-error)))
      (is (= ks @missing)))))
