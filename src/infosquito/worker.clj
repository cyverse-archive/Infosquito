(ns infosquito.worker
  (:require [clojure.data.json :as dj]
            [clojure.tools.logging :as tl]
            [clojurewerkz.elastisch.query :as ceq]
            [clojurewerkz.elastisch.rest.response :as cerr]
            [com.github.drsnyder.beanstalk :as cgdb]
            [clj-jargon.jargon :as cj]
            [clojure-commons.file-utils :as cf]
            [infosquito.es-if :as e]))


(def ^{:private true} index "iplant")
(def ^{:private true} file-type "file")
(def ^{:private true} dir-type "folder")


;; IRODS Functions


(defn- get-mapping-type
  [irods entry-path]
  (if (cj/is-file? irods entry-path) file-type dir-type))


;; Work Queue Functions


(def ^{:private true} index-entry-task "index entry")
(def ^{:private true} index-members-task "index members")
(def ^{:private true} remove-entry-task "remove entry")


(defn- mk-task
  [type path]
  {:type type :path path}) 


(defn- post-task
  [worker task]
  (let [msg (dj/json-str task)]
    (cgdb/put (:queue worker) 0 0 (:task-ttr worker) (count msg) msg)))


;; Indexer Functions


(defn- mk-index-doc
  [user path]
  {:user user :name (cf/basename path)})


(defn- mk-index-id
  [entry-path]
  (cf/rm-last-slash entry-path))


;; Work Logic


(defn- index-entry
  [worker path]
  (let [user (second (re-matches #"^/iplant/home/([^/]+)(/.*)?$" path))]
    (cj/with-jargon (:irods-cfg worker) [irods]
      (when (cj/is-readable? irods user path)
        (e/put (:indexer worker) 
              index 
              (get-mapping-type irods path) 
              (mk-index-id path) 
              (mk-index-doc user path))))))


(defn- index-members
  [worker dir-path]
  (cj/with-jargon (:irods-cfg worker) [irods]
    (doseq [entry (cj/list-paths irods dir-path)]
      (post-task worker (mk-task index-entry-task entry))
      (when (and (cj/is-dir? irods entry)
                 (not (cj/is-linked-dir? irods entry)))
        (post-task worker (mk-task index-members-task entry))))))
  
  
(defn- remove-entry
  [worker path]
  (let [indexer (:indexer worker)
        id      (mk-index-id path)]
    (e/delete indexer index file-type id)
    (e/delete indexer index dir-type id)))


(defn- remove-missing-entries
  [worker]
  ;; TODO set up paging here.  Lazy sequence?
  ;; TODO schedule removal if it exists, but isn't readable
  (if (e/exists? (:indexer worker) index)
    (cj/with-jargon (:irods-cfg worker) [irods]
      (doseq [path (remove #(cj/exists? irods %)
                           (cerr/ids-from (e/search-all-types (:indexer worker) 
                                                              index 
                                                              (ceq/match-all))))]
        (post-task worker (mk-task remove-entry-task path))))))


(defn- dispatch-task
  [worker task]
  (let [path (:path task)]
    (condp = (:type task)
      index-entry-task   (index-entry worker path)
      index-members-task (index-members worker path)
      remove-entry-task  (remove-entry worker path))))


(defn mk-worker
  "Constructs the worker

   Parameters:
     irods-cfg - The irods configuration to use
     queue-client-ctor - The constructor for the queue client
     indexer - The client for the Elastic Search platform
     task-ttr - The maximum number of seconds a worker may take to perform the 
       task.

   Returns:
     A worker object"
  [irods-cfg queue-client-ctor indexer task-ttr]
  {:irods-cfg irods-cfg
   :queue     (queue-client-ctor)
   :indexer   indexer
   :task-ttr  task-ttr})


(defn process-next-task
  "Reads the next available task from the queue and performs it.  If there are
   no tasks in the queue, it will wait for the next available task.

   Parameters:
     worker - The worker performing the task."
  [worker]
  (let [queue (:queue worker)
        task  (.reserve queue)] 
    (when task 
      (dispatch-task worker (dj/read-json (:payload task)))
      (.delete queue (:id task)))))


(defn sync-index
  "Synchronizes the search index with the iRODS repository.  It removes all
   entries it finds in the index that are no longer in the repository, then it
   reindexes all of the current entries in the repository.  This function
   doesn't actually make changes to the index.  Instead it schedules tasks in
   the work queue.

   Parameters:
     worker - The worker performing the task."
  [worker]
  (tl/info "Synchronizing index with iRODS repository")
  (remove-missing-entries worker)
  (index-members worker "/iplant/home"))
