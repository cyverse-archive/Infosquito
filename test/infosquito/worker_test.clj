(ns infosquito.worker-test
  (:use clojure.test
        infosquito.worker
        infosquito.mock-beanstalk
        infosquito.mock-es)
  (:require [clojure.data.json :as json]
            [boxy.core :as boxy]
            [clj-jargon.jargon :as irods]
            [infosquito.es-if :as es]
            [infosquito.work-queue :as queue]))


(def ^{:private true} init-irods-repo
  {:users                                              #{"user1" "user2"}
   :groups                                             {}
   "/tempZone"                                         {:type :normal-dir
                                                        :acl  {}
                                                        :avus {}}
   "/tempZone/home"                                    {:type :normal-dir
                                                        :acl  {}
                                                        :avus {}}        
   "/tempZone/home/user1"                              {:type    :normal-dir
                                                        :acl     {"user1" :read}
                                                        :avus    {}}
   "/tempZone/home/user1/readable-file"                {:type    :file
                                                        :acl     {"user1" :read}
                                                        :avus    {}
                                                        :content ""}
   "/tempZone/home/user1/unreadable-file"              {:type    :file
                                                        :acl     {}
                                                        :avus    {}
                                                        :content ""}
   "/tempZone/home/user1/readable-dir"                 {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/unreadable-dir"               {:type :normal-dir
                                                        :acl  {}
                                                        :avus {}}
   "/tempZone/home/user1/unreadable-dir/readable-file" {:type    :file
                                                        :acl     {"user1" :read}
                                                        :avus    {}
                                                        :content ""}
   "/tempZone/home/user1/linked-dir"                   {:type :linked-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/linked-dir/readable-file"     {:type    :file
                                                        :acl     {"user1" :read}
                                                        :avus    {}
                                                        :content ""}
   "/tempZone/home/user2"                              {:type    :normal-dir
                                                        :acl     {"user2" :read}
                                                        :avus    {}}
   "/tempZone/home/user2/readable-file"                {:type    :file
                                                        :acl     {"user2" :read}
                                                        :avus    {}
                                                        :content ""}})


(defn- setup
  []
  (let [queue      (atom [])
        es-repo    (atom {})
        proxy-ctor #(boxy/mk-mock-proxy (atom init-irods-repo))]
    [queue 
     es-repo 
     (mk-worker (irods/init "localhost" 
                            "1297" 
                            "user" 
                            "/tempZone/home/rods" 
                            "rods" 
                            "tempZone" 
                            "dr"
                            :proxy-ctor proxy-ctor)
                (queue/mk-client #(mk-mock-beanstalk queue) 3 120)
                (mk-mock-indexer es-repo))]))


(defn- populate-queue
  [queue-ref task]
  (reset! queue-ref [{:id 0 :payload (json/json-str task)}]))
  

(deftest test-index-entry
  (testing "index readable file"
    (let [[queue-ref es-repo-ref worker] (setup)]
      (populate-queue queue-ref {:type "index entry" 
                                 :path "/tempZone/home/user1/readable-file"})
      (process-next-task worker)
      (is (empty? @queue-ref))
      (is (= (get-in @es-repo-ref 
                     ["iplant" "file" "/tempZone/home/user1/readable-file"])
             {:name "readable-file" :user "user1"}))))
  (testing "index readable folder"
    (let [[queue-ref es-repo-ref worker] (setup)]
      (populate-queue queue-ref {:type "index entry" 
                                 :path "/tempZone/home/user1/readable-dir"})
      (process-next-task worker)
      (is (= (get-in @es-repo-ref 
                     ["iplant" "folder" "/tempZone/home/user1/readable-dir"])
             {:name "readable-dir" :user "user1"}))))
  (testing "index unreadable entry"
    (let [[queue-ref es-repo-ref worker] (setup)]    
      (populate-queue queue-ref {:type "index entry" 
                                 :path "/tempZone/home/user1/unreadable-file"})
      (process-next-task worker)
      (is (nil? (get-in @es-repo-ref 
                        ["iplant" "file" "/tempZone/home/user1/unreadable-file"]))))))


(deftest test-index-members
  (let [[queue-ref es-repo-ref worker] (setup)]
    (populate-queue queue-ref {:type "index members" :path "/tempZone/home/user1"})
    (process-next-task worker)
    (is (empty? @es-repo-ref))
    (is (= (set (map #(json/read-json (:payload %)) 
                     @queue-ref)) 
           #{{:type "index entry"   :path "/tempZone/home/user1/readable-file"}
             {:type "index entry"   :path "/tempZone/home/user1/unreadable-file"}
             {:type "index entry"   :path "/tempZone/home/user1/readable-dir/"}
             {:type "index members" :path "/tempZone/home/user1/readable-dir/"}
             {:type "index entry"   :path "/tempZone/home/user1/unreadable-dir/"}
             {:type "index members" :path "/tempZone/home/user1/unreadable-dir/"}
             {:type "index entry"   :path "/tempZone/home/user1/linked-dir/"}}))))


(deftest test-remove-entry
  (let [[queue-ref es-repo-ref worker] (setup)]
    (reset! es-repo-ref 
            {"iplant" {"file" {"/tempZone/home/user1/old-file" {:name "old-file" 
                                                                :user "user1"}}}})
    (populate-queue queue-ref 
                    {:type "remove entry" :path "/tempZone/home/user1/old-file"})
    (process-next-task worker)
    (is (= @es-repo-ref {"iplant" {"file" {}}}))))


(deftest test-sync
  (let [[queue-ref es-repo-ref worker] (setup)]
    (reset! es-repo-ref 
            {"iplant" {"folder" {"/tempZone/home/old-user" {:name "old-user" 
                                                            :user "old-user"}
                                 "/tempZone/home/user1"    {:name "user1"
                                                            :user "user1"}}}})
    (sync-index worker)
    (is (= (set (map #(json/read-json (:payload %)) 
                @queue-ref))
           #{{:type "remove entry"  :path "/tempZone/home/old-user"}
             {:type "index entry"   :path "/tempZone/home/user1/"}
             {:type "index members" :path "/tempZone/home/user1/"}
             {:type "index entry"   :path "/tempZone/home/user2/"}
             {:type "index members" :path "/tempZone/home/user2/"}}))))
