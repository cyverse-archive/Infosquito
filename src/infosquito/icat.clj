(ns infosquito.icat
  (:use [clojure.pprint :only [pprint]]
        [infosquito.progress :only [notifier]])
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure-commons.file-utils :as file]
            [infosquito.es :as es]
            [infosquito.es-if :as es-if]))

(def ^:private index "data")
(def ^:private file-type "file")
(def ^:private dir-type "folder")


(defn- mk-acl-query
  [entry-id]
  (str "SELECT u.user_name  \"username\",
               u.zone_name  \"zone\",
               t.token_name \"permission\"
          FROM r_objt_access AS a
            LEFT JOIN r_user_main AS u ON a.user_id = u.user_id
            LEFT JOIN r_tokn_main AS t ON a.access_type_id = t.token_id
          WHERE a.object_id = " entry-id))


(defn- mk-colls-query
  [coll-base]
  (str "SELECT coll_id                                         \"db-id\",
               coll_name                                       \"id\",
               REPLACE(coll_name, parent_coll_name || '/', '') \"label\",
               coll_owner_name                                 \"creator-name\",
               coll_owner_zone                                 \"creator-zone\",
               CAST(create_ts AS bigint)                       \"date-created\",
               CAST(modify_ts AS bigint)                       \"date-modified\"
          FROM r_coll_main
          WHERE coll_name LIKE '" coll-base "/%' AND coll_type = ''"))


(defn- mk-data-objs-query
  [coll-base]
  (str "SELECT DISTINCT d.data_id                           \"db-id\",
                        (c.coll_name || '/' || d.data_name) \"id\",
                        d.data_name                         \"label\",
                        d.data_owner_name                   \"creator-name\",
                        d.data_owner_zone                   \"creator-zone\",
                        CAST(d.create_ts AS bigint)         \"date-created\",
                        CAST(d.modify_ts AS bigint)         \"date-modified\",
                        d.data_size                         \"file-size\",
                        d.data_type_name                    \"info-type\"
          FROM r_data_main AS d LEFT JOIN r_coll_main AS c ON d.coll_id = c.coll_id
          WHERE c.coll_name LIKE '" coll-base "/%'"))


(defn- mk-meta-query
  [entry-id]
  (str "SELECT meta_attr_name  \"attribute\",
               meta_attr_value \"value\",
               meta_attr_unit  \"unit\"
          FROM r_meta_main
          WHERE meta_id IN (SELECT meta_id FROM r_objt_metamap WHERE object_id = " entry-id ")"))


(defn- query-paged
  [cfg sql-query result-receiver]
  (sql/transaction (sql/with-query-results*
                     [{:fetch-size (:result-page-size cfg)} sql-query]
                     result-receiver)))


(defn- get-acl
  [cfg entry-id acl-receiver]
  (query-paged cfg (mk-acl-query entry-id) acl-receiver))


(defn- get-colls-wo-acls
  [cfg coll-receiver]
  (query-paged cfg (mk-colls-query (:collection-base cfg)) coll-receiver))


(defn- get-data-objs-wo-acls
  [cfg data-obj-receiver]
  (query-paged cfg (mk-data-objs-query (:collection-base cfg)) data-obj-receiver))


(defn- get-metadata
  [cfg entry-id meta-receiver]
  (query-paged cfg (mk-meta-query entry-id) meta-receiver))


(defn- attach-acl
  [cfg entry]
  (assoc entry :user-permissions (get-acl cfg (:db-id entry) (partial mapv identity))))


(defn- attach-metadata
  [cfg entry]
  (assoc entry :metadata (get-metadata cfg (:db-id entry) (partial mapv identity))))


(defn- attach-with
  [attach entry-provider entry-receiver]
  (entry-provider (comp entry-receiver (partial map attach))))


(defn- map-acls
  [cfg entry-provider entry-receiver]
  (attach-with (partial attach-acl cfg) entry-provider entry-receiver))


(defn- map-metadata
  [cfg entry-provider entry-receiver]
  (attach-with (partial attach-metadata cfg) entry-provider entry-receiver))


(defn init
  "This function creates the map containing the configuration parameters.

   Parameters:
     icat-host       - the name of the server hosting the ICAT database
     icat-port       - the ICAT port number (defaults to '5432')
     icat-db         - the name of the ICAT database (defaults to 'ICAT')
     icat-user       - the ICAT database user name
     icat-password   - the ICAT database user password
     collection-base - the root collection contain all the entries of interest
     es-url          - the base URL to use when connecting to ElasticSearch"
  [{:keys [icat-host icat-port icat-db icat-user icat-password collection-base es-url]
    :or   {icat-port "5432"
           icat-db   "ICAT"
           es-port   "9200"}}]
  {:collection-base  collection-base
   :icat-host        icat-host
   :icat-port        icat-port
   :icat-db          icat-db
   :icat-user        icat-user
   :icat-password    icat-password
   :result-page-size 80
   :es-url           es-url})


(defn get-db-spec
  "Generates a database connection specification from a configuration map."
  [{:keys [icat-host icat-port icat-db icat-user icat-password]}]
  {:subprotocol "postgresql"
   :subname     (str "//" icat-host ":" icat-port "/" icat-db)
   :user        icat-user
   :password    icat-password})


(defmacro with-icat
  "This opens the database connection and excecutes the provided operation within a transaction.
   Operating within a transaction ensures that the autocommit is off, allowing a cursor to be used
   on the database.  This in turn allows for large result sets that don't fit in memory.

   Parameters:
     cfg - The configuration mapping
     ops - The operations that will be executed on the open connection."
  [cfg & ops]
  `(sql/with-connection (get-db-spec ~cfg) ~@ops))


(defn get-collections
  "Given an open connection, this function executes appropriate queries on the connection requesting
   all collections under the collection-base with each collection having an attached ACL. It passes
   a lazy stream of collections to the given continuation following CPS.

   Parameters:
     cfg - the configuration mapping
     coll-receiver - The continuation that will receive the stream of collections

   Returns:
     It returns whatever the continuation returns."
  [cfg coll-receiver]
  (map-metadata cfg (partial map-acls cfg (partial get-colls-wo-acls cfg)) coll-receiver))


(defn get-data-objects
  "Given an open connection, this function executes appropriate queries on the connection requesting
   all data objects under the collection-base with each data object having an attached ACL. It
   passes a lazy stream of data objects to the given continuation following CPS.

   Parameters:
     cfg - the configuration mapping
     obj-receiver - The continuation that will receive the stream of data objects"
  [cfg obj-receiver]
  (map-metadata cfg (partial map-acls cfg (partial get-data-objs-wo-acls cfg)) obj-receiver))


(defn- mk-index-doc
  [entry-type entry]
  (let [fmt-perm   (fn [perm] (condp = perm
                               "read object"   "read"
                               "modify object" "write"
                               "own"           "own"
                               nil))
        fmt-user   (fn [name zone] (str name "#" zone))
        fmt-access (fn [access] {:permission (fmt-perm (:permission access))
                                :user       (fmt-user (:username access) (:zone access))})
        doc        {:id              (:id entry)
                    :userPermissions (map fmt-access (:user-permissions entry))
                    :creator         (fmt-user (:creator-name entry) (:creator-zone entry))
                    :dateCreated     (:date-created entry)
                    :dateModified    (:date-modified entry)
                    :label           (:label entry)
                    :metadata        (:metadata entry)}]
    (if (= entry-type dir-type)
      doc
      (assoc doc
        :fileSize (:file-size entry)
        :fileType (:info-type entry)))))


(defn- index-entry
  [indexer entry-type entry]
  (try
    (es-if/put indexer index entry-type (:id entry) (mk-index-doc entry-type entry))
    (catch Exception e
      (println "Failed to index" entry-type entry ":" e))))


(def ^:private count-collections-query
  "SELECT count(*) \"count\"
     FROM r_coll_main
    WHERE coll_name LIKE ? AND coll_type = ''")


(defn- count-collections
  [cfg]
  (sql/with-query-results rs [count-collections-query (str (:collection-base cfg) "/%")]
    (:count (first rs))))


(def ^:private count-data-objects-query
  "SELECT count(d.*) \"count\"
     FROM r_data_main d
     JOIN r_coll_main c ON d.coll_id = c.coll_id
    WHERE c.coll_name like ?")


(defn- count-data-objects
  [cfg]
  (sql/with-query-results rs [count-data-objects-query (str (:collection-base cfg) "/%")]
    (:count (first rs))))


(defn- index-results
  [index-fn entries]
  (dorun (pmap (fn [chunk] (dorun (map index-fn chunk)))
               (partition-all 8 entries))))


(defn- index-collections
  [cfg indexer]
  (println "Indexing" (count-collections cfg) "collections.")
  (->> (partial index-entry indexer dir-type)
       (notifier 10000)
       (partial index-results)
       (get-collections cfg)))


(defn- index-data-objects
  [cfg indexer]
  (println "Indexing" (count-data-objects cfg) "data objects.")
  (->> (partial index-entry indexer file-type)
       (notifier 10000)
       (partial index-results)
       (get-data-objects cfg)))


(def ^:private file-existence-query
  (str "SELECT count(*) FROM r_data_main d "
       "JOIN r_coll_main c ON d.coll_id = c.coll_id "
       "WHERE c.coll_name = ? "
       "AND d.data_name = ?"))


(defn file-exists?
  [path]
  (let [dir-path  (string/replace path #"/[^/]+$" "")
        file-name (string/replace path #"^.*/" "")]
    (sql/with-query-results rs [file-existence-query dir-path file-name]
      ((comp pos? :count first) rs))))


(def ^:private folder-existence-query
  (str "SELECT count(*) FROM r_coll_main "
       "WHERE coll_name = ?"))


(defn folder-exists?
  [path]
  (sql/with-query-results rs [folder-existence-query path]
    ((comp pos? :count first) rs)))


(defn reindex
  [cfg]
  (let [indexer (es/mk-indexer (:es-url cfg))]
    (index-collections cfg indexer)
    (index-data-objects cfg indexer)))
