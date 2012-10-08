(ns infosquito.worker
  (:require [clojure.data.json :as dj]
            [clojure.tools.logging :as tl]
            [clojurewerkz.elastisch.query :as ceq]
            [clojurewerkz.elastisch.rest :as cer]
            [clojurewerkz.elastisch.rest.document :as cerd]
            [clojurewerkz.elastisch.rest.response :as cerr]
            [clj-jargon.jargon :as cj]
            [clojure-commons.file-utils :as cf]))


(def ^{:private true} index "iplant")
(def ^{:private true} file-type "file")
(def ^{:private true} dir-type "folder")


;; IRODS Functions


(defn- get-mapping-type
  [irods entry-path]
  (if (cj/is-file? irods entry-path) file-type dir-type))


;; Work Queue Functions


(def ^{:private true} index-entry-task "index entry")
(def ^{:private true} index-members-task "index member")
(def ^{:private true} remove-entry-task "remove entry")


(defn- mk-task
  [type path]
  {:type type :path path}) 


(defn- post-task
  [worker task]
  (let [msg (dj/json-str task)]
    (.put (:queue worker) 0 0 (:task-ttr worker) (count msg) msg)))


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
  (let [user (second (re-matches #"^/iplant/home/(.*)(/.*)?$" path))]
    (cj/with-jargon (:irods-cfg worker) [irods]
      (if (cj/is-readable? irods user path)
        (cerd/put index 
                  (get-mapping-type irods path) 
                  (mk-index-id path) 
                  (mk-index-doc user path))))))


(defn- index-members
  [worker dir-path]
  (cj/with-jargon (:irods-cfg worker) [irods]
    (doseq [entry (remove #(cj/is-linked-dir? irods %) 
                          (cj/list-paths irods dir-path))]
      (post-task worker (mk-task index-entry-task entry))
      (if (cj/is-dir? irods entry) (post-task (mk-task index-members-task entry))))))
  
  
(defn- remove-entry
  [worker path]
  (cj/with-jargon (:irods-cfg worker) [irods]
    (cerd/delete index (get-mapping-type irods path) (mk-index-id path))))


(defn- remove-missing-entries
  [worker]
  ;; TODO set up paging here.  Lazy sequence?
  (cj/with-jargon (:irods-cfg worker) [irods]
    (doseq [path (remove #(cj/exists? irods %)
                         (cerr/ids-from 
                           (cerd/search-all-types index :query (ceq/match-all))))]
      (post-task worker (mk-task remove-entry-task path)))))


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
     es-url - The URL for the Elastic Search platform
     task-ttr - The maximum number of seconds a worker may take to perform the 
       task.

   Returns:
     A worker object"
  [irods-cfg queue-client-ctor es-url task-ttr]
  (cer/connect! es-url)  
  {:irod-cfg irods-cfg
   :queue    (queue-client-ctor)
   :task-ttr task-ttr})


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
