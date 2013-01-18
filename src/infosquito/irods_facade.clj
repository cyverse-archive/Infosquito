(ns infosquito.irods-facade
  "This namespace provides a wrapper around clj-jargon.jargon/with-jargon that traps JargonException
   and JargonRuntimeException, converting them to slingshot exceptions of type :connection."
  (:require [slingshot.slingshot :as ss]
            [clj-jargon.jargon :as irods])
  (:import [org.irods.jargon.core.exception JargonException
                                            JargonRuntimeException]))


(defmacro with-irods
  [irods-cfg irods-sym & body]
  `(ss/try+
     (irods/with-jargon ~irods-cfg ~irods-sym ~@body)
     (catch JargonException e#
       (ss/throw+ {:type :connection :msg  (str "Failed to connect to iRODS. " (.getMessage e#))}))
     (catch JargonRuntimeException e#
       (ss/throw+ {:type :connection :msg  (str "Lost connection to iRODS. " (.getMessage e#))}))))
