(ns infosquito.worker-test
  (:use clojure.test
        infosquito.worker
        infosquito.mock-es)
  (:require [cheshire.core :as cheshire]
            [slingshot.slingshot :as ss]
            [boxy.core :as boxy]
            [clj-jargon.jargon :as irods]
            [clojure-commons.infosquito.mock-beanstalk :as beanstalk]
            [clojure-commons.infosquito.work-queue :as queue]
            [infosquito.es-if :as es]
            [infosquito.irods-facade :as irods-wrapper]))


(def ^{:private true} too-long-dir-name
  "/tempZone/home/user2/trash/home-rods-wregglej-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.183209331-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

(def ^{:private true} multibyte-user-name (str "bcourtois" \u00a0))

(def ^{:private true} multibyte-path (str "/tempZone/home/" multibyte-user-name))

(def ^{:private true} init-irods-repo
  {:users                                              #{"user1" "user2" multibyte-user-name}
   :groups                                             {}
   "/tempZone"                                         {:type :normal-dir
                                                        :acl  {}
                                                        :avus {}}
   "/tempZone/home"                                    {:type :normal-dir
                                                        :acl  {"user1" :read
                                                               "user2" :read}
                                                        :avus {}}
   multibyte-path                                      {:type :normal-dir
                                                        :acl  {multibyte-user-name :own}
                                                        :avus {}}
   "/tempZone/home/user1"                              {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
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
   "/tempZone/home/user1/readable-dir/dir1"            {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/dir2"            {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/dir3"            {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/dir4"            {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/dir5"            {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/dir6"            {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/dir7"            {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/dir8"            {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/dir9"            {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/dir10"           {:type :normal-dir
                                                        :acl  {"user1" :read}
                                                        :avus {}}
   "/tempZone/home/user1/readable-dir/file1"           {:type    :file
                                                        :acl     {"user1" :read}
                                                        :avus    {}
                                                        :content ""}
   "/tempZone/home/user1/readable-dir/file2"           {:type    :file
                                                        :acl     {"user1" :read}
                                                        :avus    {}
                                                        :content ""}
   "/tempZone/home/user1/readable-dir/file3"           {:type    :file
                                                        :acl     {"user1" :read}
                                                        :avus    {}
                                                        :content ""}
   "/tempZone/home/user1/readable-dir/file4"           {:type    :file
                                                        :acl     {"user1" :read}
                                                        :avus    {}
                                                        :content ""}
   "/tempZone/home/user1/readable-dir/file5"           {:type    :file
                                                        :acl     {"user1" :read}
                                                        :avus    {}
                                                        :content ""}
   "/tempZone/home/user1/readable-dir/file6"           {:type    :file
                                                        :acl     {"user1" :read}
                                                        :avus    {}
                                                        :content ""}
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
        job-ttr    120
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
                                    job-ttr
                                    "infosquito")]
    (queue/with-server queue
      (irods-wrapper/with-irods irods-cfg [irods]
        (op (mk-worker queue 
                       irods 
                       (->MockIndexer es-state-ref) 
                       "/tempZone/home" 
                       job-ttr 
                       "10m" 
                       50))))))


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
            (beanstalk/put-job 10 (cheshire/encode job))
            first)))


(defn- get-queued
  [queue-state-ref]
  (set (map #(-> % :payload (cheshire/decode true))
            (beanstalk/get-jobs @queue-state-ref "infosquito"))))


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
             (cheshire/decode true))))
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
      (is (not thrown?)))))


(deftest test-index-members
  (testing "normal operation no paging"
    (let [[queue-state-ref es-state-ref call] (setup)]
      (populate-queue queue-state-ref {:type "index members" :path "/tempZone/home/user1"})
      (call process-next-job)
      (is (has-index? @es-state-ref "iplant"))
      (is (= (get-queued queue-state-ref)
             #{{:type "index members" :path "/tempZone/home/user1/readable-dir"}
               {:type "index members" :path "/tempZone/home/user1/unreadable-dir"}}))))
  (testing "normal operation paging"
    (let [[queue-state-ref es-state-ref call] (setup)]
      (populate-queue queue-state-ref {:type "index members"
                                       :path "/tempZone/home/user1/readable-dir"})
      (call process-next-job)
      (is (= 10 (count (get-queued queue-state-ref))))
      (is (= 10 (count (get-ids @es-state-ref "iplant" "folder"))))
      (is (= 6 (count (get-ids @es-state-ref "iplant" "file"))))))
   (testing "dir name too long doesn't throw out"
    (let [[queue-state-ref _ call] (setup)]
      (populate-queue queue-state-ref {:type "index members" :path "/tempZone/home/user2/trash"})
      (is (ss/try+
            (call process-next-job)
            true
            (catch Object o false)))))
  (testing "missing directory doesn't throw out"
    (let [[queue-state-ref _ call] (setup)]
      (populate-queue queue-state-ref {:type "index members" :path "/unknown"})
      (is (ss/try+
            (call process-next-job)
            true
            (catch Object _ false)))))
  (testing "irods proxy oom doesn't throw out"
    (let [[queue-state-ref _ call] (setup :irods-proxy-ctor mk-oom-proxy)
          thrown?                  (ss/try+
                                     (populate-queue queue-state-ref
                                                     {:type "index members" :path "/zone"})
                                     (call process-next-job)
                                     false
                                     (catch Object _ true))]
      (is (not thrown?)))))


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
             {:type "index members" :path multibyte-path}
             {:type "index members" :path "/tempZone/home/user1"}
             {:type "index members" :path "/tempZone/home/user2"}}))))
