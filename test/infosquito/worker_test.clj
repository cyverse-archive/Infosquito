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
             {:name "readable-file" :user "user1"}))))
  (testing "index readable folder"
    (let [[queue-state-ref es-repo-ref worker] (setup)]
      (populate-queue queue-state-ref {:type "index entry" 
                                       :path "/tempZone/home/user1/readable-dir"})
      (process-next-task worker)
      (is (= (get-in @es-repo-ref 
                     ["iplant" "folder" "/tempZone/home/user1/readable-dir"])
             {:name "readable-dir" :user "user1"}))))
  (testing "index unreadable entry"
    (let [[queue-state-ref es-repo-ref worker] (setup)]    
      (populate-queue queue-state-ref 
                      {:type "index entry" 
                       :path "/tempZone/home/user1/unreadable-file"})
      (process-next-task worker)
      (is (nil? (get-in @es-repo-ref 
                        ["iplant" "file" "/tempZone/home/user1/unreadable-file"]))))))


(deftest test-index-members
  (let [[queue-state-ref es-repo-ref worker] (setup)]
    (populate-queue queue-state-ref {:type "index members" 
                                     :path "/tempZone/home/user1"})
    (process-next-task worker)
    (is (empty? @es-repo-ref))
    (is (= (set (map #(json/read-json (:payload %)) 
                     (get (:tubes @queue-state-ref) "infosquito"))) 
           #{{:type "index entry"   :path "/tempZone/home/user1/readable-file"}
             {:type "index entry"   :path "/tempZone/home/user1/unreadable-file"}
             {:type "index entry"   :path "/tempZone/home/user1/readable-dir/"}
             {:type "index members" :path "/tempZone/home/user1/readable-dir/"}
             {:type "index entry"   :path "/tempZone/home/user1/unreadable-dir/"}
             {:type "index members" :path "/tempZone/home/user1/unreadable-dir/"}
             {:type "index entry"   :path "/tempZone/home/user1/linked-dir/"}}))))


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
    (sync-index worker)
    (is (= (set (map #(json/read-json (:payload %)) 
                (get (:tubes @queue-state-ref) "infosquito")))
           #{{:type "remove entry"  :path "/tempZone/home/old-user"}
             {:type "index entry"   :path "/tempZone/home/user1/"}
             {:type "index members" :path "/tempZone/home/user1/"}
             {:type "index entry"   :path "/tempZone/home/user2/"}
             {:type "index members" :path "/tempZone/home/user2/"}}))))
