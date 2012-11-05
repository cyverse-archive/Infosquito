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
(def ^{:private true} sync-task "sync")


(defn- log-when-es-failed
  [op-name response]
  (if-not (cerr/ok? response)
    (log/error op-name "failed" response))
  response) 
  
  
(defn- mk-task
  [type path]
  {:type type :path path}) 


;; IRODS Functions


(defn- get-mapping-type
  [irods entry-path]
  (if (irods/is-file? irods entry-path) file-type dir-type))


(defn- get-viewers
  [irods entry-path]
  (letfn [(view? [perms] (or (:read perms)
                             (:write perms)
                             (:own perms)))]
    (->> entry-path 
      (irods/list-user-perms irods) 
      (filter #(view? (:permissions %)))
      (map :user))))

  
;; Indexer Functions


(defn- mk-index-doc
  [path viewers]
  {:name (file/basename path) :viewers viewers})


(defn- mk-index-id
  [entry-path]
  (file/rm-last-slash entry-path))


;; Work Logic


(defn- index-entry
  [worker path]
  (log/trace "indexing" path)
  (irods/with-jargon (:irods-cfg worker) [irods]
    (let [viewers (get-viewers irods path)]
      (log-when-es-failed "index entry"
                          (es/put (:indexer worker) 
                                  index 
                                  (get-mapping-type irods path) 
                                  (mk-index-id path) 
                                  (mk-index-doc path viewers))))))


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
    (log-when-es-failed "remove entry" (es/delete indexer index file-type id))
    (log-when-es-failed "remove entry" (es/delete indexer index dir-type id))))


(defn- init-index-scrolling
  [worker]
  (let [resp (es/search-all-types (:indexer worker) 
                                    index 
                                    {:search_type "scan"
                                     :scroll      (:scroll-ttl worker)
                                     :size        (:scroll-page-size worker)
                                     :query       (ceq/match-all)})]
    (if-let [scroll-id (:_scroll_id resp)]
      scroll-id
      (ss/throw+ {:type :index-scroll :resp resp}))))
 

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
            (let [resp (es/scroll indexer scroll-id (:scroll-ttl worker))]
              (if-not (:_scroll_id resp) 
                (ss/throw+ {:type :index-scroll :resp resp}))
              (when (pos? (count (cerr/hits-from resp)))
                (doseq [path (remove #(irods/exists? irods %) 
                                     (cerr/ids-from resp))]
                  (queue/put (:queue worker) 
                             (json/json-str (mk-task remove-entry-task path))))
                (recur (:_scroll_id resp))))))))
    (catch [:type :index-scroll] {:keys [resp]}
      (log/error "Stopping removal of missing entries." resp))
    (catch [:type :beanstalkd-oom] {:keys []}
      (log/warn "Stopping removal of missing entries."
                "beanstalkd is out of memory."))
    (catch [:type :beanstalkd-draining] {:keys []}
      (log/warn "Stopping removal of missing entries."
                "beanstalkd is not accepting new tasks."))))


(defn- sync-with-repo
  "Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic error 
       internal to the worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker]
  (log/info "Synchronizing index with iRODS repository")
  (remove-missing-entries worker)
  (index-members worker (:index-root worker)))


(defn- dispatch-task
  "Throws:
     :connection - This is thrown if it loses its connection to beanstalkd.
     :internal-error - This is thrown if there is an error in the logic error 
       internal to the worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker task]
  (condp = (:type task)
    index-entry-task   (index-entry worker (:path task))
    index-members-task (index-members worker (:path task))
    remove-entry-task  (remove-entry worker (:path task))
    sync-task          (sync-with-repo worker)))


(defn mk-worker
  "Constructs the worker

   Parameters:
     irods-cfg - The irods configuration to use
     queue-client - The queue client
     indexer - The client for the Elastic Search platform
     index-root - This is the absolute path into irods indicating the directory
       whose contents are to be indexed.
     scroll-ttl - The amount of time Elastic Search will persist a scan result
     scroll-page-size - The number of scan result entries to return for a single 
       scroll call.
       
   Returns:
     A worker object"
  [irods-cfg queue-client indexer index-root scroll-ttl scroll-page-size]
  {:irods-cfg        irods-cfg
   :queue            queue-client
   :indexer          indexer
   :index-root       (file/rm-last-slash index-root)
   :scroll-ttl       scroll-ttl
   :scroll-page-size scroll-page-size})


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
  (queue/with-server (:queue worker) (sync-with-repo worker)))
