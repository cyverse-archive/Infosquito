(ns infosquito.icat
  (:use clojure.pprint)
  (:require [clojure.java.jdbc :as sql]))


; TODO describe cfg and conn-params maps
; TODO describe a collection map
; TODO describe a permission map

(defn- mk-acl-query
  [entry-id]
  (str "SELECT u.user_name  \"name\", 
               u.zone_name  \"zone\", 
               t.token_name \"permission\"
          FROM r_objt_access AS a
            LEFT JOIN r_user_main AS u ON a.user_id = u.user_id
            LEFT JOIN r_tokn_main AS t ON a.access_type_id = t.token_id
          WHERE a.object_id = " entry-id))


(defn- mk-colls-query
  [coll-base]
  (str "SELECT coll_id                                         id,
               REPLACE(coll_name, parent_coll_name || '/', '') \"name\",
               parent_coll_name                                \"parent-path\",
               create_ts                                       \"create-time\", 
               modify_ts                                       \"modity-time\",
               coll_type                                       \"type\"
          FROM r_coll_main 
          WHERE coll_name LIKE '" coll-base "/%'"))


(defn- mk-data-objs-query
  [coll-base]
  (str "SELECT DISTINCT d.data_id   \"id\",
                        d.data_name \"name\",
                        c.coll_name \"parent-path\",
                        d.create_ts \"create-time\",
                        d.modify_ts \"modify-time\"
          FROM r_data_main AS d LEFT JOIN r_coll_main AS c ON d.coll_id = c.coll_id
          WHERE c.coll_name LIKE '" coll-base "/%'"))
  

(defn- mk-linked-data-objs-query
  [coll-base]
  (str "SELECT c.coll_id                                       \"id\",
               REPLACE(coll_name, parent_coll_name || '/', '') \"name\",
               c.parent_coll_name                              \"parent-path\",
               d.\"create-time\"                               \"create-time\",
               d.\"modify-time\"                               \"modify-time\"
          FROM r_coll_main AS c 
            INNER JOIN (" (mk-data-objs-query coll-base) ") AS d 
              ON c.coll_info1 = (d.\"parent-path\" || '/' || d.name)
          WHERE c.coll_name LIKE '" coll-base "/%' AND c.coll_type = 'linkPoint'"))
  
  
(defn- mk-is-link-coll-query
  [link]
  (str "SELECT COUNT(*) > 0 
          FROM r_coll_main 
          WHERE coll_name IN (SELECT coll_info1 FROM r_coll_main WHERE coll_name = '" link "')"))

  
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
 

(defn- get-linked-data-objs-wo-acls
  [cfg data-obj-receiver]
  (query-paged cfg (mk-linked-data-objs-query (:collection-base cfg)) data-obj-receiver))


(defn- is-link-coll?
  [link-path]
  (sql/with-query-results result [(mk-is-link-coll-query link-path)]
    (first result)))

  
(defn- attach-acls
  [cfg entry-provider entry-receiver]
  (letfn [(attach-acl [entry] (assoc entry :acl (get-acl cfg (:id entry) (partial mapv identity))))]    
    (entry-provider (comp entry-receiver (partial map attach-acl)))))


(defn init
  "This function creates the map containing the configuration parameters.
   
   Parameters:
     user - the ICAT database user name
     password - the ICAT database user password
     collection-base - the root collection contain all the entries of interest"
  [user password collection-base]
  {:collection-base  collection-base
   :user             user
   :password         password
   :result-page-size 100})

  
(defmacro with-icat
  "This opens the database connection and excecutes the provided operation within a transaction.
   Operating within a transaction ensures that the autocommit is off, allowing a cursor to be used
   on the database.  This in turn allows for large result sets that don't fit in memory.

   Parameters:
     cfg - The configuration mapping
     ops - The operations that will be executed on the open connection."
  [cfg & ops]
  `(sql/with-connection {:subprotocol "postgresql"
                         :subname     "ICAT"
                         :user        (:user ~cfg)
                         :password    (:password ~cfg)}
     ~@ops))


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
  (letfn [(keep? [coll] (or (empty? (:type coll))
                            (and (= (:type coll) "linkPoint") (is-link-coll? (:name coll)))))] 
    (attach-acls cfg (partial get-colls-wo-acls cfg) (comp coll-receiver (partial filter keep?)))))


(defn get-data-objects
  "Given an open connection, this function executes appropriate queries on the connection requesting
   all data objects under the collection-base with each data object having an attached ACL. It 
   passes a lazy stream of data objects to the given continuation following CPS.

   Parameters:
     cfg - the configuration mapping
     obj-receiver - The continuation that will receive the stream of data objects"
  [cfg obj-receiver]
  (attach-acls cfg (partial get-data-objs-wo-acls cfg) obj-receiver)
  (attach-acls cfg (partial get-linked-data-objs-wo-acls cfg) obj-receiver)
  nil)


; This is just a test function
(defn doit
  [cfg]
  (letfn [(print-colls [colls] (doseq [coll colls] (pprint coll)))]
    (with-icat cfg 
      (get-collections cfg print-colls)
      (get-data-objects cfg print-colls))))
