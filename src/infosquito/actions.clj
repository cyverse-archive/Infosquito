(ns infosquito.actions
  (:require [infosquito.icat :as icat]
            [infosquito.props :as props]))

(defn- props->icat-cfg
  [p]
  {:icat-host       (props/get-icat-host p)
   :icat-port       (props/get-icat-port p)
   :icat-db         (props/get-icat-db p)
   :icat-user       (props/get-icat-user p)
   :icat-password   (props/get-icat-pass p)
   :collection-base (props/get-base-collection p)
   :es-host         (props/get-es-host p)
   :es-port         (props/get-es-port p)})

(defn reindex
  [props]
  ((comp icat/reindex icat/init props->icat-cfg) props))
