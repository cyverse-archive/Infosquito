(ns infosquito.irods-utils
  (:require [clojure-commons.file-utils :as ft]
            [clojure.string :as string])
  (:import [org.irods.jargon.core.exception DataNotFoundException JargonException]
           [org.irods.jargon.core.protovalues FilePermissionEnum UserTypeEnum]
           [org.irods.jargon.core.pub.domain SpecificQueryDefinition UserFilePermission]
           [org.irods.jargon.core.query
            CollectionAndDataObjectListingEntry
            CollectionAndDataObjectListingEntry$ObjectType
            SpecificQuery]
           [org.irods.jargon.core.utils IRODSDataConversionUtil]))

(def ^:private queries
  {"collections_with_permissions"
   (str "SELECT c.parent_coll_name, c.coll_name, c.create_ts, c.modify_ts, c.coll_id, "
        "c.coll_owner_name, c.coll_owner_zone, c.coll_type, u.user_name, u.zone_name, "
        "a.access_type_id, u.user_id, u.user_type_name "
        "FROM r_coll_main c "
        "JOIN r_objt_access a ON c.coll_id = a.object_id "
        "JOIN r_user_main u ON a.user_id = u.user_id "
        "WHERE c.parent_coll_name = ? "
        "ORDER BY c.coll_name "
        "LIMIT ? "
        "OFFSET ?")

   "data_objects_with_permissions"
   (str "SELECT s.coll_name, s.data_name, s.create_ts, s.modify_ts, s.data_id, s.data_size, "
        "s.data_owner_name, u.user_name, u.zone_name, a.access_type_id, u.user_id, "
        "u.user_type_name FROM "
        "(SELECT c.coll_name, d.data_name, d.create_ts, d.modify_ts, d.data_id, d.data_size, "
        "d.data_owner_name "
        "FROM r_coll_main c "
        "JOIN r_data_main d ON c.coll_id = d.coll_id "
        "WHERE c.coll_name = ? "
        "ORDER BY d.data_name) s "
        "JOIN r_objt_access a ON s.data_id = a.object_id "
        "JOIN r_user_main u ON a.user_id = u.user_id "
        "LIMIT ? "
        "OFFSET ?")})

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

(defn- user-file-perms
  [rows offset]
  (mapv
   (fn [row]
     (UserFilePermission.
      (row offset)
      (row (+ 3 offset))
      (FilePermissionEnum/valueOf (Integer/parseInt (row (+ 2 offset))))
      (UserTypeEnum/findTypeByString (row (+ 4 offset)))
      (row (+ 1 offset))))
   rows))

(defn- coll-listing
  [rows count]
  (when-let [row (first rows)]
    (doto (CollectionAndDataObjectListingEntry.)
      (.setParentPath         (row 0))
      (.setObjectType         (CollectionAndDataObjectListingEntry$ObjectType/COLLECTION))
      (.setPathOrName         (row 1))
      (.setCreatedAt          (IRODSDataConversionUtil/getDateFromIRODSValue (row 2)))
      (.setModifiedAt         (IRODSDataConversionUtil/getDateFromIRODSValue (row 3)))
      (.setId                 (IRODSDataConversionUtil/getIntOrZeroFromIRODSValue (row 4)))
      (.setOwnerName          (row 5))
      (.setOwnerZone          (row 6))
      (.setSpecColType        (IRODSDataConversionUtil/getCollectionTypeFromIRODSValue (row 7)))
      (.setCount              count)
      (.setUserFilePermission (user-file-perms rows 8)))))

(defn- data-object-listing
  [rows count]
  (when-let [row (first rows)]
    (doto (CollectionAndDataObjectListingEntry.)
      (.setParentPath (row 0))
      (.setObjectType (CollectionAndDataObjectListingEntry$ObjectType/DATA_OBJECT))
      (.setPathOrName (row 1))
      (.setCreatedAt  (IRODSDataConversionUtil/getDateFromIRODSValue (row 2)))
      (.setModifiedAt (IRODSDataConversionUtil/getDateFromIRODSValue (row 3)))
      (.setId         (IRODSDataConversionUtil/getIntOrZeroFromIRODSValue (row 4)))
      (.setDataSize   (IRODSDataConversionUtil/getLongOrZeroFromIRODSValue (row 5)))
      (.setOwnerName  (row 6))
      (.setCount      count)
      (.setUserFilePermission (user-file-perms rows 7)))))

(defn- lazy-listing
  [get-listing get-id curr-rows count [row & rows]]
  (let [curr-id  (fn [] (when (seq curr-rows) (get-id (first curr-rows))))
        next-id  (fn [] (get-id row))
        last-elm (fn [] (if-let [elm (get-listing curr-rows count)] (vector elm) []))
        rec-call (partial lazy-listing get-listing get-id)]
    (cond
     (nil? row)              (last-elm)
     (empty? curr-rows)      (rec-call [row] count rows)
     (= (curr-id) (next-id)) (rec-call (conj curr-rows row) count rows)
     :else                   (cons
                              (get-listing curr-rows count)
                              (lazy-seq (rec-call [row] (inc count) rows))))))

(defn list-subcollections
  [irods path]
  (lazy-listing
   coll-listing #(% 4) [] 0
   (execute-specific-query irods "collections_with_permissions" 5000 path)))

(defn list-data-objects
  [irods path]
  (lazy-listing
   data-object-listing #(% 4) [] 0
   (execute-specific-query irods "data_objects_with_permissions" 5000 path)))
