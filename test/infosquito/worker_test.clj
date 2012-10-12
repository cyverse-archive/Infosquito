(ns infosquito.worker-test
  (:use clojure.test
        infosquito.worker
        infosquito.mock-beanstalk
        infosquito.mock-es)
  (:require [clojure.data.json :as dj]
            [boxy.core :as bc]
            [clj-jargon.jargon :as cj]
            [infosquito.es-if :as e]))


(def ^{:private true} init-irods-repo
  {:users                                            #{"user1" "user2"}
   :groups                                           {}
   "/iplant"                                         {:type :normal-dir
                                                      :acl  {}
                                                      :avus {}}
   "/iplant/home"                                    {:type :normal-dir
                                                      :acl  {}
                                                      :avus {}}        
   "/iplant/home/user1"                              {:type    :normal-dir
                                                      :acl     {"user1" :read}
                                                      :avus    {}}
   "/iplant/home/user1/readable-file"                {:type    :file
                                                      :acl     {"user1" :read}
                                                      :avus    {}
                                                      :content ""}
   "/iplant/home/user1/unreadable-file"              {:type    :file
                                                      :acl     {}
                                                      :avus    {}
                                                      :content ""}
   "/iplant/home/user1/readable-dir"                 {:type :normal-dir
                                                      :acl  {"user1" :read}
                                                      :avus {}}
   "/iplant/home/user1/unreadable-dir"               {:type :normal-dir
                                                      :acl  {}
                                                      :avus {}}
   "/iplant/home/user1/unreadable-dir/readable-file" {:type    :file
                                                      :acl     {"user1" :read}
                                                      :avus    {}
                                                      :content ""}
   "/iplant/home/user1/linked-dir"                   {:type :linked-dir
                                                      :acl  {"user1" :read}
                                                      :avus {}}
   "/iplant/home/user1/linked-dir/readable-file"     {:type    :file
                                                      :acl     {"user1" :read}
                                                      :avus    {}
                                                      :content ""}
   "/iplant/home/user2"                              {:type    :normal-dir
                                                      :acl     {"user2" :read}
                                                      :avus    {}}
   "/iplant/home/user2/readable-file"                {:type    :file
                                                      :acl     {"user2" :read}
                                                      :avus    {}
                                                      :content ""}})


(defn- setup
  []
  (let [queue   (atom [])
        es-repo (atom {})]
    [queue 
     es-repo 
     (mk-worker (cj/init "localhost" 
                         "1297" 
                         "user" 
                         "/zone/home/user" 
                         "crackme" 
                         "zone" 
                         "dr"
                         :proxy-ctor #(bc/mk-mock-proxy (atom init-irods-repo)))
                #(mk-mock-beanstalk queue)
                (->MockIndexer es-repo) 
                120)]))


(defn- populate-queue
  [queue-ref task]
  (reset! queue-ref [{:id 0 :payload (dj/json-str task)}]))
  

(deftest test-index-entry
  (testing "index readable file"
    (let [[queue-ref es-repo-ref worker] (setup)]
      (populate-queue queue-ref {:type "index entry" 
                                 :path "/iplant/home/user1/readable-file"})
      (process-next-task worker)
      (is (empty? @queue-ref))
      (is (= (get-in @es-repo-ref 
                     ["iplant" "file" "/iplant/home/user1/readable-file"])
             {:name "readable-file" :user "user1"}))))
  (testing "index readable folder"
    (let [[queue-ref es-repo-ref worker] (setup)]
      (populate-queue queue-ref {:type "index entry" 
                                 :path "/iplant/home/user1/readable-dir"})
      (process-next-task worker)
      (is (= (get-in @es-repo-ref 
                     ["iplant" "folder" "/iplant/home/user1/readable-dir"])
             {:name "readable-dir" :user "user1"}))))
  (testing "index unreadable entry"
    (let [[queue-ref es-repo-ref worker] (setup)]    
      (populate-queue queue-ref {:type "index entry" 
                                 :path "/iplant/home/user1/unreadable-file"})
      (process-next-task worker)
      (is (nil? (get-in @es-repo-ref 
                        ["iplant" "file" "/iplant/home/user1/unreadable-file"]))))))


(deftest test-index-members
  (let [[queue-ref es-repo-ref worker] (setup)]
    (populate-queue queue-ref {:type "index members" :path "/iplant/home/user1"})
    (process-next-task worker)
    (is (empty? @es-repo-ref))
    (is (= (set (map #(dj/read-json (:payload %)) 
                     @queue-ref)) 
           #{{:type "index entry"   :path "/iplant/home/user1/readable-file"}
             {:type "index entry"   :path "/iplant/home/user1/unreadable-file"}
             {:type "index entry"   :path "/iplant/home/user1/readable-dir/"}
             {:type "index members" :path "/iplant/home/user1/readable-dir/"}
             {:type "index entry"   :path "/iplant/home/user1/unreadable-dir/"}
             {:type "index members" :path "/iplant/home/user1/unreadable-dir/"}
             {:type "index entry"   :path "/iplant/home/user1/linked-dir/"}}))))


(deftest test-remove-entry
  (let [[queue-ref es-repo-ref worker] (setup)]
    (reset! es-repo-ref 
            {"iplant" {"file" {"/iplant/home/user1/old-file" {:name "old-file" 
                                                              :user "user1"}}}})
    (populate-queue queue-ref 
                    {:type "remove entry" :path "/iplant/home/user1/old-file"})
    (process-next-task worker)
    (is (= @es-repo-ref {"iplant" {"file" {}}}))))


(deftest test-sync
  (let [[queue-ref es-repo-ref worker] (setup)]
    (reset! es-repo-ref 
            {"iplant" {"folder" {"/iplant/home/old-user" {:name "old-user" 
                                                          :user "old-user"}
                                 "/iplant/home/user1"    {:name "user1"
                                                          :user "user1"}}}})
    (sync-index worker)
    (is (= (set (map #(dj/read-json (:payload %)) 
                @queue-ref))
           #{{:type "remove entry"  :path "/iplant/home/old-user"}
             {:type "index entry"   :path "/iplant/home/user1/"}
             {:type "index members" :path "/iplant/home/user1/"}
             {:type "index entry"   :path "/iplant/home/user2/"}
             {:type "index members" :path "/iplant/home/user2/"}}))))
  