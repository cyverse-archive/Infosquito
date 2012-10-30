(ns infosquito.worker
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojurewerkz.elastisch.query :as ceq]
            [clojurewerkz.elastisch.rest :as cer]
            [clojurewerkz.elastisch.rest.response :as cerr]
            [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as irods]
            [clojure-commons.file-utils :as file]
            [infosquito.es-if :as es]
            [infosquito.work-queue :as queue]))


(def ^{:private true} index "iplant")
(def ^{:private true} file-type "file")
(def ^{:private true} dir-type "folder")


(def ^{:private true} index-entry-task "index entry")
(def ^{:private true} index-members-task "index members")
(def ^{:private true} remove-entry-task "remove entry")


(defn- mk-task
  [type path]
  {:type type :path path}) 


;; IRODS Functions


(defn- get-mapping-type
  [irods entry-path]
  (if (irods/is-file? irods entry-path) file-type dir-type))


;; Indexer Functions


(defn- mk-index-doc
  [user path]
  {:user user :name (file/basename path)})


(defn- mk-index-id
  [entry-path]
  (file/rm-last-slash entry-path))


;; Work Logic


(defn- index-entry
  [worker path]
  (log/trace "indexing" path)
  (let [user (second (re-matches #"^/tempZone/home/([^/]+)(/.*)?$" path))]
    (irods/with-jargon (:irods-cfg worker) [irods]
      (when (irods/is-readable? irods user path)
        (es/put (:indexer worker) 
                index 
                (get-mapping-type irods path) 
                (mk-index-id path) 
                (mk-index-doc user path))))))


(defn- index-members
  "Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic error 
       internal to the work queue.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker dir-path]
  (log/trace "indexing the members of" dir-path)
  (ss/try+ 
    (let [queue (:queue worker)]
      (irods/with-jargon (:irods-cfg worker) [irods]
        (doseq [entry (irods/list-paths irods dir-path)]
          (queue/put queue (json/json-str (mk-task index-entry-task entry)))
          (when (and (irods/is-dir? irods entry)
                     (not (irods/is-linked-dir? irods entry)))
            (queue/put queue (json/json-str (mk-task index-members-task entry)))))))
    (catch [:type :beanstalkd-oom] {:keys []}
      (log/warn "Stopping indexing members of" dir-path "."
                "beanstalkd is out of memory."))
    (catch [:type :beanstalkd-draining] {:keys []}
      (log/warn "Stopping indexing members of" dir-path "."
                "beanstalkd is not accepting new tasks."))))
  
  
(defn- remove-entry
  [worker path]
  (log/trace "removing" path)
  (let [indexer (:indexer worker)
        id      (mk-index-id path)]
    (es/delete indexer index file-type id)
    (es/delete indexer index dir-type id)))

  

(defn- init-index-scrolling
  [worker]
  (:_scroll_id (es/search-all-types (:indexer worker) 
                                    index 
                                    {:search_type "scan"
                                     :scroll      "10m" 
                                     :size        50    
                                     :query       (ceq/match-all)})))
 

(defn- remove-missing-entries
  "Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic error 
       internal to the worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker]
  (ss/try+
    (let [indexer (:indexer worker)]
      (when (es/exists? indexer index)
        (irods/with-jargon (:irods-cfg worker) [irods]
          (loop [scroll-id (init-index-scrolling worker)]
            (let [resp (es/scroll indexer scroll-id "10m")]  
              (when (pos? (count (cerr/hits-from resp)))
                (doseq [path (remove #(irods/exists? irods %) 
                                     (cerr/ids-from resp))]
                  (queue/put (:queue worker) 
                             (json/json-str (mk-task remove-entry-task path))))
                (recur (:_scroll_id resp))))))))
    (catch [:type :beanstalkd-oom] {:keys []}
      (log/warn "Stopping removal of missing entries."
                "beanstalkd is out of memory."))
    (catch [:type :beanstalkd-draining] {:keys []}
      (log/warn "Stopping removal of missing entries."
                "beanstalkd is not accepting new tasks."))))


(defn- dispatch-task
  "Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic error 
       internal to the worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
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
     queue-client - The queue client
     indexer - The client for the Elastic Search platform

   Returns:
     A worker object"
  [irods-cfg queue-client indexer]
  {:irods-cfg irods-cfg
   :queue     queue-client
   :indexer   indexer})


(defn process-next-task
  "Reads the next available task from the queue and performs it.  If there are
   no tasks in the queue, it will wait for the next available task.

   Parameters:
     worker - The worker performing the task.

   Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic error 
       internal to the worker.
     :unknown-error - This is thrown if an unidentifiable error occurs.
     :beanstalkd-oom - This is thrown if beanstalkd is out of memory."
  [worker]
  (let [queue (:queue worker)]
    (queue/with-server queue
      (when-let [task (queue/reserve queue)]  
        (dispatch-task worker (json/read-json (:payload task)))
        (queue/delete queue (:id task))))))


(defn sync-index
  "Synchronizes the search index with the iRODS repository.  It removes all
   entries it finds in the index that are no longer in the repository, then it
   reindexes all of the current entries in the repository.  This function
   doesn't actually make changes to the index.  Instead it schedules tasks in
   the work queue.

   Parameters:
     worker - The worker performing the task.

   Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic error 
       internal to the worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker]
  (log/info "Synchronizing index with iRODS repository")
  (queue/with-server (:queue worker)
    (remove-missing-entries worker)
    (index-members worker "/tempZone/home")))
