(ns infosquito.worker
  (:require [cheshire.core :as cheshire]
            [clojure.tools.logging :as log]
            [clojurewerkz.elastisch.query :as ceq]
            [clojurewerkz.elastisch.rest :as cer]
            [clojurewerkz.elastisch.rest.response :as cerr]
            [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as irods]
            [clj-jargon.lazy-listings :as lazyrods]
            [clojure-commons.file-utils :as file]
            [clojure-commons.infosquito.work-queue :as queue]
            [infosquito.es-if :as es]
            [infosquito.exceptions :as exn])
  (:import [org.irods.jargon.core.exception FileNotFoundException
                                            JargonException]
           [org.irods.jargon.core.protovalues FilePermissionEnum]
           [org.irods.jargon.core.pub.domain ObjStat$SpecColType]))


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


(defn- log-missing-entry
  [entry]
  (log/debug "Skipping missing entry" entry))


(defn- mk-job
  [type path]
  {:type type :path path})


;; IRODS Functions

(defn- get-entry
  "throws:
     :missing-irods-entry - This is thrown entry-path doesn't point to a valid entry in iRODS"
  [irods entry-path]
  (ss/try+
    (let [entry (.getCollectionAndDataObjectListingEntryAtGivenAbsolutePath (:lister irods)
                                                                            entry-path)
          acl   (if (.isDataObject entry)
                  (.listPermissionsForDataObject (:dataObjectAO irods) entry-path)
                  (.listPermissionsForCollection (:collectionAO irods) entry-path))]
      (doto entry (.setUserFilePermission acl)))
    (catch FileNotFoundException _ (ss/throw+ {:type :missing-irods-entry :entry entry-path}))))


(defn- get-mapping-type
  [entry]
  (if (.isDataObject entry) file-type dir-type))


(defn- validate-path
  [abs-path]
  (ss/try+
    (irods/validate-path-lengths abs-path)
    (catch [:error_code irods/ERR_BAD_DIRNAME_LENGTH] {:keys [full-path]}
      (ss/throw+ {:type :bad-path
                  :dir  full-path
                  :msg  "The parent directory path is too long"}))
    (catch [:error_code irods/ERR_BAD_BASENAME_LENGTH] {:keys [full-path]}
      (ss/throw+ {:type :bad-path
                  :dir  full-path
                  :msg  "The directory name is too long"}))
    (catch [:error_code irods/ERR_BAD_PATH_LENGTH] {:keys [full-path]}
      (ss/throw+ {:type :bad-path
                  :dir  full-path
                  :msg  "The directory path is too long"}))))

(defn- visit-listing-entry
  [visit entry]
  (let [path (.getFormattedAbsolutePath entry)]
    (ss/try+
     (validate-path path)
     (visit entry)
     (catch [:type :bad-path] {:keys [msg]}
       (log/warn "skipping" path "-" msg)))
    entry))

;; Indexer Functions


(defn- mk-index-doc
  [entry]
  (letfn [(fmt-perm      [perm] (condp = perm
                                  FilePermissionEnum/READ  "read"
                                  FilePermissionEnum/WRITE "write"
                                  FilePermissionEnum/OWN   "own"
                                                           nil))
          (fmt-acl-entry [acl-entry] {:name       (.getUserName acl-entry)
                                      :zone       (.getUserZone acl-entry)
                                      :permission (fmt-perm (.getFilePermissionEnum acl-entry))})]
    {:name        (.getNodeLabelDisplayValue entry)
     :parent_path (.getParentPath entry)
     :creator     {:name (.getOwnerName entry) :zone (.getOwnerZone entry)}
     :create_date (.getTime (.getCreatedAt entry))
     :modify_date (.getTime (.getModifiedAt entry))
     :acl         (map fmt-acl-entry (.getUserFilePermission entry))}))


(defn- mk-index-id
  [path]
  (file/rm-last-slash path))


;; Work Logic


(defn- index-entry
  [worker entry]
  (log/trace "indexing" (.getFormattedAbsolutePath entry))
  (let [indexer (:indexer worker)
        type    (get-mapping-type entry)
        id      (mk-index-id (.getFormattedAbsolutePath entry))
        doc     (mk-index-doc entry)]
    (log-when-es-failed "index entry"
                        (es/put indexer index type id doc))))


(defn- index-entry-path
  [worker path]
  (ss/try+
    (index-entry worker (get-entry (:irods worker) path))
    (catch [:type :missing-irods-entry] {:keys [entry]} (log-missing-entry entry))))


(defn- index-collection
  [worker collection]
  (ss/try+
    (index-entry worker collection)
    (when-not (= ObjStat$SpecColType/LINKED_COLL (.getSpecColType collection))
      (queue/put
       (:queue worker)
       (cheshire/encode (mk-job index-members-job (.getFormattedAbsolutePath collection)))))
    (catch [:type :permission-denied] {:keys [msg entry]} (log/warn "skipping" entry "-" msg))
    (catch [:type :missing-irods-entry] {:keys [entry]} (log-missing-entry entry))))


(defn- index-data-object
  [worker data-object]
  (ss/try+
    (index-entry worker data-object)
    (catch [:type :missing-irods-entry] {:keys [entry]} (log-missing-entry entry))))

(defn- try-listing
  [get-listing dir-path]
  (ss/try+
   (get-listing dir-path)
   (catch FileNotFoundException _ (ss/throw+ {:type :missing-irods-entry :entry dir-path}))
   (catch JargonException _ (ss/throw+ {:type :listing-error :entry dir-path}))))

(defn- list-subdirs-in
  [irods dir-path]
  (try-listing (partial lazyrods/list-subdirs-in irods) dir-path))

(defn- list-files-in
  [irods dir-path]
  (try-listing (partial lazyrods/list-files-in irods) dir-path))

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
      (let [irods (:irods worker)]
        (validate-path dir-path)
        (dorun (map (partial visit-listing-entry (partial index-collection worker))
                    (list-subdirs-in irods dir-path)))
        (dorun (map (partial visit-listing-entry (partial index-data-object worker))
                    (list-files-in irods dir-path))))
      (catch [:type :bad-path] {:keys [msg]} (log-stop-warn msg))
      (catch [:type :permission-denied] {:keys [msg]} (log-stop-warn msg))
      (catch [:type :missing-irods-entry] {}
        (log/debug "Stopping indexing members of" dir-path "because it doesn't exist anymore.")))))


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
              (doseq [path (remove #(irods/exists? (:irods worker) %) (cerr/ids-from resp))]
                (queue/put (:queue worker) (cheshire/encode (mk-job remove-entry-job path))))
              (recur (:_scroll_id resp)))))))
    (catch [:type :index-scroll] {:keys [resp]}
      (log/error "Stopping removal of missing entries." resp))))


(defn- sync-index
  "Throws:
     :connection - This is thrown if it loses a required connection.
     :internal-error - This is thrown if there is an error in the logic error internal to the
       worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker]
  (log/info "Synchronizing index with iRODS repository")
  (remove-missing-entries worker)
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
      index-entry-job   (index-entry-path worker (:path job))
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
     job-ttr - The number of seconds before beanstalk with expire a job reservation.
     scroll-ttl - The amount of time Elastic Search will persist a scan result
     scroll-page-size - The number of scan result entries to return for a single scroll call.
     retry-delay - the number of seconds to wait before retrying a task.

   Returns:
     A worker object"
  [queue-client irods indexer index-root job-ttr scroll-ttl scroll-page-size retry-delay]
  {:queue            queue-client
   :irods            irods
   :indexer          indexer
   :index-root       (file/rm-last-slash index-root)
   :job-ttr          job-ttr
   :scroll-ttl       scroll-ttl
   :scroll-page-size scroll-page-size
   :retry-delay      retry-delay})


(defn- repeatedly-renew
  [worker job]
  (let [delay-time-ms  (-> (:job-ttr worker) (* 1000) (/ 2))
        renew-in-a-bit (fn [] (Thread/sleep delay-time-ms)
                              (queue/touch (:queue worker) (:id job))
                              (log/debug "renewed current job reservation"))]
    (ss/try+
      (dorun (repeatedly renew-in-a-bit))
      (catch [:type :connection] {:keys [msg]} (log/error "connection failure." msg))
      (catch InterruptedException _)
      (catch Object _ (log/error (exn/fmt-throw-context &throw-context))))))


(defn process-next-job
  "Reads the next available job from the queue and performs it.  If there are no jobs in the queue,
   it will wait for the next available job.

   Parameters:
     worker - The worker performing the job.

   Throws:
     :connection - This is thrown if it loses a required connection.
     :internal-error - This is thrown if there is an error in the logic error internal to the
       worker.
     :unknown-error - This is thrown if an unidentifiable error occurs."
  [worker]
  (let [queue (:queue worker)]
    (when-let [job (queue/reserve queue)]
      (let [renewer (future (repeatedly-renew worker job))]
      (ss/try+
        (dispatch-job worker (cheshire/decode (:payload job) true))
        (future-cancel renewer)
        (queue/delete queue (:id job))
        (catch [:type :listing-error] {:keys [dir-path]}
          (log/error "error encountered while obtaining listings for" dir-path
                     "- moving directory task to the end of the queue")
          (queue/delete queue (:id job))
          (queue/put queue (:payload job) :delay (:retry-delay worker)))
        (finally
          (ss/try+
            (when-not (future-cancelled? renewer) (future-cancel renewer))
            (queue/release queue (:id job))
            (catch Object _))))))))
