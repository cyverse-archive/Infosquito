(ns infosquito.core
  "This namespace defines the entry point for Infosquito. All state should be in here."
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :as ss]
            [clojure-commons.config :as config]
            [infosquito.exceptions :as exn]
            [infosquito.props :as props])
  (:import [java.util Properties]))


(defn- update-props
  [load-props props]
    (let [props-ref (ref props
                         :validator (fn [p] (if (empty? p)
                                              true
                                              (props/validate p #(log/error %&)))))]
      (ss/try+
        (load-props props-ref)
        (catch Object _
          (log/error "Failed to load configuration parameters.")))
      (when (.isEmpty @props-ref)
        (ss/throw+ {:type :cfg-problem
                    :msg  "Don't have any configuration parameters."}))
      (when-not (= props @props-ref) (config/log-config props-ref))
      @props-ref))


(defmacro ^{:private true} trap-exceptions!
  [& body]
  `(ss/try+
     (do ~@body)
     (catch Object o# (log/error (exn/fmt-throw-context ~'&throw-context)))))


(defn -main
  [& args])
