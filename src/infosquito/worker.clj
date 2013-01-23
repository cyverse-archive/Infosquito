(ns infosquito.worker
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [clojurewerkz.elastisch.query :as ceq]
            [clojurewerkz.elastisch.rest :as cer]
            [clojurewerkz.elastisch.rest.response :as cerr]
            [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as irods]
            [clojure-commons.file-utils :as file]
            [clojure-commons.infosquito.work-queue :as queue]
            [infosquito.es-if :as es])
  (:import [org.irods.jargon.core.exception FileNotFoundException]))


(def ^{:private true} index "iplant")
(def ^{:private true} file-type "file")
(def ^{:private true} dir-type "folder")


(def ^{:private true} index-entry-job "index entry")
(def ^{:private true} index-members-job "index members")
(def ^{:private true} remove-entry-job "remove entry")
(def ^{:private true} sync-job "sync")


(defn- log-when-es-failed
  [op-name response]
  (if-not (cerr/ok? response)
    (log/error op-name "failed" response))
  response) 
  
  
(defn- mk-job
  [type path]
  {:type type :path path}) 


;; IRODS Functions

(defn- get-mapping-type
  "throws:
     :missing-irods-entry - This is thrown entry-path doesn't point to a valid
       entry in iRODS"
  [irods entry-path]
  (ss/try+
    (if (irods/is-file? irods entry-path) file-type dir-type)
    (catch FileNotFoundException _
      (ss/throw+ {:type :missing-irods-entry :entry entry-path}))))


(defn- get-members
  "throws:
    :bad-dir-path - This is thrown if there is something wrong with the provided directory path.
    :missing-irods-entry - This is thrown if the directory doesn't exists.
    :oom - This is thrown if the VM cannot allocate enough to store the paths of all of the 
      members."
  [irods dir-path]
  (ss/try+
    (let [members (irods/list-paths irods dir-path :ignore-child-exns)]
      (when (some nil? members)
        (log/warn "Ignoring members of" dir-path "that have names that are too long."))
      (remove nil? members))
    (catch [:error_code irods/ERR_BAD_DIRNAME_LENGTH] {:keys [full-path]}
      (ss/throw+ {:type :bad-dir-path 
                  :dir  full-path
                  :msg  "The parent directory path is too long"}))
    (catch [:error_code irods/ERR_BAD_BASENAME_LENGTH] {:keys [full-path]}
      (ss/throw+ {:type :bad-dir-path 
                  :dir  full-path
                  :msg  "The directory name is too long"}))
    (catch [:error_code irods/ERR_BAD_PATH_LENGTH] {:keys [full-path]}
      (ss/throw+ {:type :bad-dir-path 
                  :dir  full-path
                  :msg  "The directory path is too long"}))
    (catch FileNotFoundException _ (ss/throw+ {:type :missing-irods-entry :entry dir-path}))
    (catch OutOfMemoryError _  (ss/throw+ {:type :oom :entry dir-path}))))
    

(defn- get-viewers
  "throws:
     :missing-irods-entry - This is thrown entry-path doesn't point to a valid
       entry in iRODS"
  [irods entry-path]
  (letfn [(view? [perms] (or (:read perms)
                             (:write perms)
                             (:own perms)))]
    (ss/try+
      (->> entry-path 
        (irods/list-user-perms irods) 
        (filter #(view? (:permissions %)))
        (map :user))
      (catch FileNotFoundException _
        (ss/throw+ {:type :missing-irods-entry :entry entry-path})))))

  
;; Indexer Functions


(defn- mk-index-doc
  [path viewers]
  {:name (file/basename path) :viewers viewers})


(defn- mk-index-id
  [entry-path]
  (file/rm-last-slash entry-path))


;; Work Logic


(defn- index-entry
  "Throws:
     :connection - This is thrown if it fails to connect to iRODS."
  ([worker path] 
    (index-entry worker path (get-mapping-type (:irods worker) path)))
  ([worker path type]
    (log/trace "indexing" path)
    (ss/try+
      (log-when-es-failed "index entry"
                          (es/put (:indexer worker) 
                                  index 
                                  type 
                                  (mk-index-id path) 
                                  (mk-index-doc path (get-viewers (:irods worker) path))))
      (catch [:type :missing-irods-entry] {:keys [entry]}
        (log/debug "Not indexing missing iRODS entry" entry)))))


(defn- index-members
  "Throws:
     :connection - This is thrown if it loses a required connection.
     :internal-error - This is thrown if there is an error in the logic error internal to the work 
       queue.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker dir-path]
  (log/trace "indexing the members of" dir-path)
  (letfn [(log-stop-warn [reason] (log/warn (str "Stopping indexing members of " dir-path ". " 
                                                 reason)))]
    (ss/try+ 
      (let [queue (:queue worker)
            irods (:irods worker)]
        (doseq [entry (get-members irods dir-path)]
          (let [folder? (irods/is-dir? irods entry)]
            (index-entry worker entry (if folder? dir-type file-type))
            (when (and folder?
                       (not (irods/is-linked-dir? irods entry)))
              (queue/put queue (json/json-str (mk-job index-members-job entry)))))))
      (catch [:type :beanstalkd-oom] {} (log-stop-warn "beanstalkd is out of memory."))
      (catch [:type :beanstalkd-draining] {} 
        (log-stop-warn "beanstalkd is not accepting new jobs."))
      (catch [:type :bad-dir-path] {:keys [msg]} (log-stop-warn msg))
      (catch [:type :missing-irods-entry] {}
        (log/debug "Stopping indexing members of" dir-path "because it doesn't exist anymore."))
      (catch [:type :oom] {} 
        (log-stop-warn "The VM cannot allocate enough memory to hold the paths to all the"
                       "members.")))))
  
  
(defn- remove-entry
  [worker path]
  #_(log/trace "removing" path)
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
     :connection - This is thrown if it loses a required connection.
     :internal-error - This is thrown if there is an error in the logic error internal to the 
       worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker]
  (ss/try+
    (let [indexer (:indexer worker)]
      (when (es/exists? indexer index)
        (loop [scroll-id (init-index-scrolling worker)]
          (let [resp (es/scroll indexer scroll-id (:scroll-ttl worker))]
            (when-not (:_scroll_id resp) (ss/throw+ {:type :index-scroll :resp resp}))
            (when (pos? (count (cerr/hits-from resp)))
              ; TODO renew reservation
              (doseq [path (remove #(irods/exists? (:irods worker) %) (cerr/ids-from resp))]
                (queue/put (:queue worker) (json/json-str (mk-job remove-entry-job path))))
              (recur (:_scroll_id resp)))))))
    (catch [:type :index-scroll] {:keys [resp]}
      (log/error "Stopping removal of missing entries." resp))
    (catch [:type :beanstalkd-oom] {}
      (log/warn "Stopping removal of missing entries. beanstalkd is out of memory."))
    (catch [:type :beanstalkd-draining] {}
      (log/warn "Stopping removal of missing entries. beanstalkd is not accepting new jobs."))))


(defn sync-index
  "Synchronizes the search index with the iRODS repository.  It removes all entries it finds in the 
   index that are no longer in the repository, then it reindexes all of the current entries in the 
   repository.  This function doesn't actually make changes to the index.  Instead it schedules jobs 
   in the work queue.

   Parameters:
     worker - The worker performing the job.

   Throws:
     :connection - This is thrown if it loses a required connection.
     :internal-error - This is thrown if there is an error in the logic error internal to the 
       worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker]
  (log/info "Synchronizing index with iRODS repository")
  (remove-missing-entries worker)
  ; TODO renew reservation
  (index-members worker (:index-root worker)))


(defn- dispatch-job
  "Throws:
     :connection - This is thrown if it loses a required connection.
     :internal-error - This is thrown if there is an error in the logic error internal to the 
       worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker job]
  (let [type (:type job)]
    (condp = type
      index-entry-job   (index-entry worker (:path job))
      index-members-job (index-members worker (:path job))
      remove-entry-job  (remove-entry worker (:path job))
      sync-job          (sync-index worker)
                        (log/warn "ignoring unknown job" type))))


(defn mk-worker
  "Constructs the worker

   Parameters:
     queue-client - the connected queue client
     irods - The irods proxy
     indexer - The client for the Elastic Search platform
     index-root - This is the absolute path into irods indicating the directory whose contents are 
       to be indexed.
     scroll-ttl - The amount of time Elastic Search will persist a scan result
     scroll-page-size - The number of scan result entries to return for a single scroll call.
       
   Returns:
     A worker object"
  [queue-client irods indexer index-root scroll-ttl scroll-page-size]
  {:queue            queue-client
   :irods            irods
   :indexer          indexer
   :index-root       (file/rm-last-slash index-root)
   :scroll-ttl       scroll-ttl
   :scroll-page-size scroll-page-size})


(defn process-next-job
  "Reads the next available job from the queue and performs it.  If there are no jobs in the queue, 
   it will wait for the next available job.

   Parameters:
     worker - The worker performing the job.

   Throws:
     :connection - This is thrown if it loses a required connection.
     :internal-error - This is thrown if there is an error in the logic error internal to the 
       worker.
     :unknown-error - This is thrown if an unidentifiable error occurs.
     :beanstalkd-oom - This is thrown if beanstalkd is out of memory."
  [worker]
  (let [queue (:queue worker)]
    (when-let [job (queue/reserve queue)]
      (ss/try+
        (dispatch-job worker (json/read-json (:payload job)))
        (queue/delete queue (:id job))
        (catch Object _
          (ss/try+ 
            (queue/release queue (:id job))
            (catch Object o))
          (ss/throw+))))))
