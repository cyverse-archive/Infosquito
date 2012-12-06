(ns infosquito.worker-test
  (:use clojure.test
        clojure-commons.infosquito.mock-beanstalk
        infosquito.worker
        infosquito.mock-es)
  (:require [clojure.data.json :as json]
            [slingshot.slingshot :as ss]
            [boxy.core :as boxy]
            [clj-jargon.jargon :as irods]
            [infosquito.es-if :as es]
            [clojure-commons.infosquito.work-queue :as queue]))


(def ^{:private true} too-long-dir-name 
  "/tempZone/home/user2/trash/home-rods-wregglej-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.183209331-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

  
(def ^{:private true} init-irods-repo
  {:users                                              #{"user1" "user2"}
   :groups                                             {}
   "/tempZone"                                         {:type :normal-dir
                                                        :acl  {}
                                                        :avus {}}
   "/tempZone/home"                                    {:type :normal-dir
                                                        :acl  {"user1" :read 
                                                               "user2" :read}
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
                                                        :content ""}
   "/tempZone/home/user2/trash"                        {:type :normal-dir
                                                        :acl  {}
                                                        :avus {}}
   too-long-dir-name                                   {:type :normal-dir
                                                        :acl  {}
                                                        :avus {}}})


(defn- setup
  []
  (let [queue-state (atom default-state)
        es-repo     (atom {})
        proxy-ctor  #(boxy/mk-mock-proxy (atom init-irods-repo))]
    [queue-state 
     es-repo 
     (mk-worker (irods/init "localhost" 
                            "1297" 
                            "user" 
                            "/tempZone/home/rods" 
                            "rods" 
                            "tempZone" 
                            "dr"
                            :proxy-ctor proxy-ctor)
                (queue/mk-client #(mk-mock-beanstalk queue-state) 
                                 3 
                                 120 
                                 "infosquito")
                (mk-mock-indexer es-repo)
                "/tempZone/home"
                "10m"
                50)]))


(defn- populate-queue
  [queue-state-ref task]
  (swap! queue-state-ref 
         #(assoc % :tubes {"infosquito" [{:id 0 :payload (json/json-str task)}]})))
  

(deftest test-index-entry
  (testing "index readable file"
    (let [[queue-state-ref es-repo-ref worker] (setup)]
      (populate-queue queue-state-ref 
                      {:type "index entry" 
                       :path "/tempZone/home/user1/readable-file"})
      (process-next-task worker)
      (is (empty? (:queue @queue-state-ref)))
      (is (= (get-in @es-repo-ref 
                     ["iplant" "file" "/tempZone/home/user1/readable-file"])
             {:name "readable-file" :viewers ["user1"]}))))
  (testing "index readable folder"
    (let [[queue-state-ref es-repo-ref worker] (setup)]
      (populate-queue queue-state-ref {:type "index entry" 
                                       :path "/tempZone/home/user1/readable-dir"})
      (process-next-task worker)
      (is (= (get-in @es-repo-ref 
                     ["iplant" "folder" "/tempZone/home/user1/readable-dir"])
             {:name "readable-dir" :viewers ["user1"]}))))
  (testing "index unreadable entry"
    (let [[queue-state-ref es-repo-ref worker] (setup)]    
      (populate-queue queue-state-ref 
                      {:type "index entry" 
                       :path "/tempZone/home/user1/unreadable-file"})
      (process-next-task worker)
      (is (= (get-in @es-repo-ref 
                     ["iplant" "file" "/tempZone/home/user1/unreadable-file"])
             {:name "unreadable-file" :viewers []}))))
  (testing "index multiple viewers"
    (let [[queue-state-ref es-repo-ref worker] (setup)]    
      (populate-queue queue-state-ref {:type "index entry" :path "/tempZone/home"})
      (process-next-task worker)
      (is (= (->> ["iplant" "folder" "/tempZone/home"] 
               (get-in @es-repo-ref) 
               :viewers 
               set)
             #{"user1" "user2"}))))
  (testing "missing entry"
    (let [[queue-state-ref _ worker] (setup)
          thrown?                    (ss/try+
                                       (populate-queue queue-state-ref 
                                                       {:type "index entry" 
                                                        :path "/missing"})
                                       (process-next-task worker)
                                       false
                                       (catch Object _
                                         true))]
      (is (not thrown?)))))
  

(deftest test-index-members
  (testing "normal operation"
    (let [[queue-state-ref es-repo-ref worker] (setup)]
      (populate-queue queue-state-ref {:type "index members" 
                                       :path "/tempZone/home/user1"})
      (process-next-task worker)
      (is (empty? @es-repo-ref))
      (is (= (set (map #(json/read-json (:payload %)) 
                       (get (:tubes @queue-state-ref) "infosquito"))) 
             #{{:type "index entry"   
                :path "/tempZone/home/user1/readable-file"}
               {:type "index entry"   
                :path "/tempZone/home/user1/unreadable-file"}
               {:type "index entry"   
                :path "/tempZone/home/user1/readable-dir/"}
               {:type "index members" 
                :path "/tempZone/home/user1/readable-dir/"}
               {:type "index entry"   
                :path "/tempZone/home/user1/unreadable-dir/"}
               {:type "index members" 
                :path "/tempZone/home/user1/unreadable-dir/"}
               {:type "index entry"   
                :path "/tempZone/home/user1/linked-dir/"}}))))
  (testing "dir name too long doesn't throw out"
    (let [[queue-state-ref _ worker] (setup)]
      (populate-queue queue-state-ref {:type "index members"
                                       :path "/tempZone/home/user2/trash"})
      (is (ss/try+
            (process-next-task worker)
            true
            (catch Object _ false)))))
  (testing "missing directory doesn't throw out"
    (let [[queue-state-ref _ worker] (setup)]
      (populate-queue queue-state-ref {:type "index members" :path "/unknown"})
      (is (ss/try+
            (process-next-task worker)
            true
            (catch Object _ false))))))


(deftest test-remove-entry
  (let [[queue-state-ref es-repo-ref worker] (setup)]
    (reset! es-repo-ref 
            {"iplant" {"file" {"/tempZone/home/user1/old-file" {:name "old-file" 
                                                                :user "user1"}}}})
    (populate-queue queue-state-ref 
                    {:type "remove entry" :path "/tempZone/home/user1/old-file"})
    (process-next-task worker)
    (is (= @es-repo-ref {"iplant" {"file" {}}}))))


(deftest test-sync
  (let [[queue-state-ref es-repo-ref worker] (setup)]
    (reset! es-repo-ref 
            {"iplant" {"folder" {"/tempZone/home/old-user" {:name "old-user" 
                                                            :user "old-user"}
                                 "/tempZone/home/user1"    {:name "user1"
                                                            :user "user1"}}}})
    (populate-queue queue-state-ref {:type "sync"})
    (process-next-task worker)
    (is (= (set (map #(json/read-json (:payload %)) 
                (get (:tubes @queue-state-ref) "infosquito")))
           #{{:type "remove entry"  :path "/tempZone/home/old-user"}
             {:type "index entry"   :path "/tempZone/home/user1/"}
             {:type "index members" :path "/tempZone/home/user1/"}
             {:type "index entry"   :path "/tempZone/home/user2/"}
             {:type "index members" :path "/tempZone/home/user2/"}}))))


(deftest test-sync-index
  (let [[queue-state-ref es-repo-ref worker] (setup)]
    (reset! es-repo-ref 
            {"iplant" {"folder" {"/tempZone/home/old-user" {:name "old-user" 
                                                            :user "old-user"}
                                 "/tempZone/home/user1"    {:name "user1"
                                                            :user "user1"}}}})
    (sync-index worker)
    (is (= (set (map #(json/read-json (:payload %)) 
                (get (:tubes @queue-state-ref) "infosquito")))
           #{{:type "remove entry"  :path "/tempZone/home/old-user"}
             {:type "index entry"   :path "/tempZone/home/user1/"}
             {:type "index members" :path "/tempZone/home/user1/"}
             {:type "index entry"   :path "/tempZone/home/user2/"}
             {:type "index members" :path "/tempZone/home/user2/"}}))))
