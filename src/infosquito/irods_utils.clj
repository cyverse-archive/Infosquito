(ns infosquito.irods-utils
  (:import [org.irods.jargon.core.exception DataNotFoundException JargonException]
           [org.irods.jargon.core.packinstr SpecificQueryInp]
           [org.irods.jargon.core.pub SpecificQueryAOImpl]
           [org.irods.jargon.core.pub.domain SpecificQueryDefinition]
           [org.irods.jargon.core.query
            CollectionAndDataObjectListingEntry
            CollectionAndDataObjectListingEntry$ObjectType
            QueryResultProcessingUtils
            SpecificQuery
            SpecificQueryResultSet]
           [org.irods.jargon.core.utils IRODSDataConversionUtil]))

(def ^:private queries
  {"collections_with_permissions"
   (str "SELECT c.parent_coll_name, c.coll_name, c.create_ts, c.modify_ts, "
        "       c.coll_id, c.coll_owner_name, c.coll_owner_zone, c.coll_type, "
        "       u.user_name, u.zone_name, a.access_type_id, u.user_id "
        "FROM r_coll_main c "
        "JOIN r_objt_access a ON c.coll_id = a.object_id "
        "JOIN r_user_main u ON a.user_id = u.user_id "
        "WHERE c.parent_coll_name = ? "
        "ORDER BY c.coll_name "
        "LIMIT ? "
        "OFFSET ? ")})

(defn- get-query-ao
  [irods]
  (.getSpecificQueryAO (:accessObjectFactory irods) (:irodsAccount irods)))

(defn- define-query
  [query-ao [alias query]]
  (try
    (.addSpecificQuery query-ao (SpecificQueryDefinition. alias query))
    (catch JargonException _)))

(defn define-specific-queries
  [irods]
  (dorun (map (partial define-query (get-query-ao irods)) queries)))

(defn- delete-query
  [query-ao [alias query]]
  (.removeSpecificQuery query-ao (SpecificQueryDefinition. alias query)))

(defn delete-specific-queries
  [irods]
  (dorun (map (partial delete-query (get-query-ao irods))  queries)))

(defn- get-specific-query-page
  [irods alias offset limit args]
  (let [query-ao  (get-query-ao irods)
        query-str (queries alias)
        args      (conj (vec (or args [])) (str limit) (str offset))
        query     (SpecificQuery/instanceArguments query-str args 0)]
    (try
      (->> (.executeSpecificQueryUsingSql query-ao query limit)
           (.getResults)
           (map #(vec (.getColumnsAsList %))))
      (catch DataNotFoundException _ []))))

(defn execute-specific-query
  [irods alias page-size & args]
  (letfn [(get-seq [offset]
            (let [page (get-specific-query-page irods alias offset page-size args)]
              (if (seq page)
                (lazy-cat page (get-seq (+ offset page-size)))
                [])))]
    (get-seq 0)))

(defn specific-queries-enabled?
  [irods]
  (let [account (:irodsAccount irods)
        env     (.getEnvironmentalInfoAO (:accessObjectFactory irods) account)]
    (.isAbleToRunSpecificQuery env)))

(defn- coll-perms
  [rows])

(defn- coll-listing
  [rows count]
  (when-let [row (first rows)]
    (doto (CollectionAndDataObjectListingEntry.)
      (.setParentPath           (row 0))
      (.setObjectType           (CollectionAndDataObjectListingEntry$ObjectType/COLLECTION))
      (.setPathOrName           (row 1))
      (.setCreatedAt            (IRODSDataConversionUtil/getDateFromIRODSValue (row 2)))
      (.setModifiedAt           (IRODSDataConversionUtil/getDateFromIRODSValue (row 3)))
      (.setId                   (IRODSDataConversionUtil/getIntOrZeroFromIRODSValue (row 4)))
      (.setOwnerName            (row 5))
      (.setOwnerZone            (row 6))
      (.setSpecColltype         (IRODSDataConversionUtil/getCollectionTypeFromIRODSValue (row 7)))
      (.setCount                count)
      (.setUserFilePermissions) (coll-perms rows))))

(defn- list-subcollections*
  [curr-rows count [row & rows]]
  (letfn [(curr-path [] (get-in curr-rows [0 1]))
          (next-path [] (row 1))]
    (cond
     (nil? row)                  (coll-listing curr-rows count)
     (empty? curr-rows)          (list-subcollections* [row] count rows)
     (= (curr-path) (next-path)) (list-subcollections* (conj curr-rows row) count rows)
     :else                       (cons (coll-listing curr-rows count)
                                       ((lazy-seq list-subcollections* [] (inc count) rows))))))

(defn list-subcollections
  [irods path]
  (list-subcollections*
   [] 0
   (execute-specific-query irods "collections_with_permissions" 5000 path)))
