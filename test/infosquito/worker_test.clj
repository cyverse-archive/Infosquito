(ns infosquito.worker-test
  (:use clojure.test
        infosquito.worker
        infosquito.mock-es)
  (:require [clojure.data.json :as json]
            [slingshot.slingshot :as ss]
            [boxy.core :as boxy]
            [clj-jargon.jargon :as irods]
            [clojure-commons.infosquito.mock-beanstalk :as beanstalk]
            [clojure-commons.infosquito.work-queue :as queue]
            [infosquito.es-if :as es]
            [infosquito.irods-facade :as irods-wrapper]))


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


(defn- mk-oom-proxy 
  [repo-ref]
  (letfn [(mk-ao-factory [] (boxy/->MockAOFactory #(boxy/->MockFileSystemAO repo-ref % true)
                                                  (partial boxy/->MockEntryListAO repo-ref)
                                                  (partial boxy/->MockCollectionAO repo-ref)
                                                  (partial boxy/->MockDataObjectAO repo-ref)
                                                  (partial boxy/->MockGroupAO repo-ref)
                                                  (partial boxy/->MockUserAO repo-ref)
                                                  (partial boxy/->MockQuotaAO repo-ref)))]
    (boxy/->MockProxy mk-ao-factory (partial boxy/mk-mock-file-factory repo-ref))))


(defn- populate-es
  [es-state-ref contents]
  (swap! es-state-ref #(set-contents % contents)))


(defn- perform-op
  [queue-state-ref es-state-ref irods-proxy-ctor op]
  (let [proxy-ctor #(irods-proxy-ctor (atom init-irods-repo))
        irods-cfg  (irods/init "localhost" 
                                "1297" 
                                "user" 
                                "/tempZone/home/rods" 
                                "rods" 
                                "tempZone" 
                                "dr"
                                :proxy-ctor proxy-ctor)
        queue      (queue/mk-client #(beanstalk/mk-mock-beanstalk queue-state-ref) 
                                    3 
                                    120 
                                    "infosquito")]
    (queue/with-server queue
      (irods-wrapper/with-irods irods-cfg [irods] 
        (op (mk-worker queue irods (->MockIndexer es-state-ref) "/tempZone/home" "10m" 50))))))


(defn- setup
  [& [& {:keys [irods-proxy-ctor] :or {irods-proxy-ctor boxy/mk-mock-proxy}}]]
  (let [queue-state (atom beanstalk/default-state) 
        es-state    (atom (mk-indexer-state))]
    [queue-state es-state (partial perform-op queue-state es-state irods-proxy-ctor)]))


(defn- populate-queue
  [queue-state-ref job]
  (swap! queue-state-ref 
         #(-> % 
            (beanstalk/use-tube "infosquito")
            (beanstalk/put-job 10 (json/json-str job))
            first)))


(defn- get-queued
  [queue-state-ref]
  (set (map #(-> % :payload json/read-json) (beanstalk/get-jobs @queue-state-ref "infosquito")))) 


(deftest test-release-on-fail
  (let [job                                 {:type "index entry" 
                                             :path "/tempZone/home/user1/readable-file"}
        [queue-state-ref es-state-ref call] (setup)]
    (populate-queue queue-state-ref job)
    (swap! es-state-ref #(fail-ops % true))
    (ss/try+
      (call process-next-job)
      (catch Object _))
    (is (= job
           (-> @queue-state-ref 
             (beanstalk/peek-ready "infosquito") 
             :payload 
             json/read-json)))
    (is (= (not (has-index? @es-state-ref "iplant"))))))

  
(deftest test-index-entry
  (testing "index readable file"
    (let [[queue-state-ref es-state-ref call] (setup)]
      (populate-queue queue-state-ref 
                      {:type "index entry" :path "/tempZone/home/user1/readable-file"})
      (call process-next-job)
      (is (not (beanstalk/jobs? @queue-state-ref)))
      (is (= (get-doc @es-state-ref "iplant" "file" "/tempZone/home/user1/readable-file")
             {:name "readable-file" :viewers ["user1"]}))))
  (testing "index readable folder"
    (let [[queue-state-ref es-state-ref call] (setup)]
      (populate-queue queue-state-ref 
                      {:type "index entry" :path "/tempZone/home/user1/readable-dir"})
      (call process-next-job)
      (is (= (get-doc @es-state-ref "iplant" "folder" "/tempZone/home/user1/readable-dir")
             {:name "readable-dir" :viewers ["user1"]}))))
  (testing "index unreadable entry"
    (let [[queue-state-ref es-state-ref call] (setup)]    
      (populate-queue queue-state-ref 
                      {:type "index entry" 
                       :path "/tempZone/home/user1/unreadable-file"})
      (call process-next-job)
      (is (= (get-doc @es-state-ref "iplant" "file" "/tempZone/home/user1/unreadable-file")
             {:name "unreadable-file" :viewers []}))))
  (testing "index multiple viewers"
    (let [[queue-state-ref es-state-ref call] (setup)]    
      (populate-queue queue-state-ref {:type "index entry" :path "/tempZone/home"})
      (call process-next-job)
      (is (= (-> @es-state-ref 
               (get-doc "iplant" "folder" "/tempZone/home") 
               :viewers 
               set)
             #{"user1" "user2"}))))
  (testing "missing entry"
    (let [[queue-state-ref _ call] (setup)
          thrown?                  (ss/try+
                                     (populate-queue queue-state-ref 
                                                     {:type "index entry" :path "/missing"})
                                     (call process-next-job)
                                     false
                                     (catch Object _ true))]
      (is (not thrown?))))
  (testing "irods proxy oom"
    (let [[queue-state-ref _ call] (setup :irods-proxy-ctor mk-oom-proxy)
          thrown?                  (ss/try+
                                     (populate-queue queue-state-ref 
                                                     {:type "index entry" :path "/missing"})
                                     (call process-next-job)
                                     false
                                     (catch Object _ true))]
      (is (not thrown?)))))


(deftest test-index-members
  (testing "normal operation"
    (let [[queue-state-ref es-state-ref call] (setup)]
      (populate-queue queue-state-ref {:type "index members" :path "/tempZone/home/user1"})
      (call process-next-job)
      (is (has-index? @es-state-ref "iplant"))
      (is (= (get-queued queue-state-ref)
             #{{:type "index members" :path "/tempZone/home/user1/readable-dir/"}
               {:type "index members" :path "/tempZone/home/user1/unreadable-dir/"}}))))
  (testing "dir name too long doesn't throw out"
    (let [[queue-state-ref _ call] (setup)]
      (populate-queue queue-state-ref {:type "index members" :path "/tempZone/home/user2/trash"})
      (is (ss/try+
            (call process-next-job)
            true
            (catch Object _ false)))))
  (testing "missing directory doesn't throw out"
    (let [[queue-state-ref _ call] (setup)]
      (populate-queue queue-state-ref {:type "index members" :path "/unknown"})
      (is (ss/try+
            (call process-next-job)
            true
            (catch Object _ false))))))


(deftest test-remove-entry
  (let [path                                "/tempZone/home/user1/old-file"
        [queue-state-ref es-state-ref call] (setup)]
    (populate-es es-state-ref {"iplant" {"file" {path {:name "old-file" :user "user1"}}}})
    (populate-queue queue-state-ref {:type "remove entry" :path path})
    (call process-next-job)
    (is (not (indexed? @es-state-ref "iplant" "file" path)))))


(deftest test-sync
  (let [[queue-state-ref es-state-ref call] (setup)]
    (populate-es es-state-ref 
                 {"iplant" {"folder" {"/tempZone/home/old-user" {:name "old-user" :user "old-user"}
                                      "/tempZone/home/user1"    {:name "user1" :user "user1"}}}})
    (populate-queue queue-state-ref {:type "sync"})
    (call process-next-job) 
    (is (= (get-queued queue-state-ref)
           #{{:type "remove entry" :path "/tempZone/home/old-user"}
             {:type "index members" :path "/tempZone/home/user1/"}
             {:type "index members" :path "/tempZone/home/user2/"}})))) 
  
  
(deftest test-sync-index
  (let [[queue-state-ref es-state-ref call] (setup)]
    (populate-es es-state-ref 
                 {"iplant" {"folder" {"/tempZone/home/old-user" {:name "old-user" :user "old-user"}
                                      "/tempZone/home/user1"    {:name "user1" :user "user1"}}}})
    (call sync-index)
    (is (= (get-queued queue-state-ref)
           #{{:type "remove entry" :path "/tempZone/home/old-user"}
             {:type "index members" :path "/tempZone/home/user1/"}
             {:type "index members" :path "/tempZone/home/user2/"}}))))
