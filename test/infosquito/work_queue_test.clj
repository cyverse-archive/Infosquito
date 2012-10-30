(ns infosquito.work-queue-test
  (:use clojure.test
        infosquito.work-queue
        infosquito.mock-beanstalk)
  (:require [clojure.data.json :as json]
            [slingshot.slingshot :as ss]))


(deftest test-mk-client
  (let [ctor   (fn [])
        client (mk-client ctor 1 2)]
    (is (= ctor (:ctor client)))
    (is (= 1 (:conn-tries client)))
    (is (= 2 (:task-ttr client)))
    (is (nil? @(:beanstalk client)))))


(deftest test-with-server
  (let [client (mk-client #(mk-mock-beanstalk (atom [])) 1 2)]
    (with-server client
      (is (not= nil @(:beanstalk client))))
    (is (nil? @(:beanstalk client)))))


(deftest test-with-server-bad-connection
   (let [state  (atom (assoc default-state :closed? true)) 
         client (mk-client #(mk-mock-beanstalk (atom []) state) 1 2)
         thrown (ss/try+
                  (with-server client)
                  false
                  (catch [:type :connection] {:keys []}
                    true))]
     (is thrown)))
 
  
(deftest test-delete
  (let [queue  (atom [{:id 0 :payload (json/json-str {})}])
        client (mk-client #(mk-mock-beanstalk queue) 1 2)]
    (with-server client
      (delete client 0))
    (is (empty? @queue))))


(deftest test-delete-bad-connection
  (let [state  (atom (assoc default-state :closed? true))
        queue  (atom [{:id 0 :payload (json/json-str {})}])
        client (mk-client #(mk-mock-beanstalk queue state) 1 2)
        thrown (ss/try+
                 (with-server client
                   (swap! state #(assoc % :closed? true))
                   (delete client 0))
                 false
                 (catch [:type :connection] {:keys []}
                   true))]
    (is thrown)))


(deftest test-put
  (let [queue   (atom [])
        client  (mk-client #(mk-mock-beanstalk queue) 1 2)
        payload (json/json-str {})]
    (with-server client
      (put client payload))
    (is (= @queue [{:id 0 :payload payload}]))))


(deftest test-put-oom
  (let [state  (atom (assoc default-state :oom? true))
        queue  (atom [])
        client (mk-client #(mk-mock-beanstalk queue state) 1 2)
        thrown (ss/try+
                 (with-server client
                   (put client (json/json-str {})))
                 false
                 (catch [:type :beanstalkd-oom] {:keys []}
                   true))]
    (is thrown)))


(deftest test-put-drain
  (let [state  (atom (assoc default-state :draining? true))
        queue  (atom [])
        client (mk-client #(mk-mock-beanstalk queue state) 1 2)
        thrown (ss/try+
                 (with-server client
                   (put client (json/json-str {})))
                 false
                 (catch [:type :beanstalkd-draining] {:keys []}
                   true))]
    (is thrown)))


(deftest test-put-bury
  (let [state  (atom (assoc default-state :bury? true))
        queue  (atom [])
        client (mk-client #(mk-mock-beanstalk queue state) 1 2)
        thrown (ss/try+
                 (with-server client
                   (put client (json/json-str {})))
                 false
                 (catch [:type :beanstalkd-oom] {:keys []}
                   true))]
    (is thrown)))


(deftest test-reserve
  (let [task   {:id 0 :payload (json/json-str {})}
        queue  (atom [task])
        client (mk-client #(mk-mock-beanstalk queue) 1 2)]
    (with-server client
      (is (= task (reserve client))))))
