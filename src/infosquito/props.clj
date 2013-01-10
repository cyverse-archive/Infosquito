(ns infosquito.props
  "This namespace holds all of the logic for managing the configuration values"
  (:import [java.net URL]))


(defn- get-int-prop
  [props name]
  (Integer/parseInt (get props name)))


(defn get-beanstalk-host
  [props]
  (get props "infosquito.beanstalk.host"))
  

(defn get-beanstalk-job-ttr
  [props]
  (get-int-prop props "infosquito.beanstalk.job-ttr"))

  
(defn get-beanstalk-port
  [props]
  (get-int-prop props "infosquito.beanstalk.port"))
  

(defn get-beanstalk-tube
  [props]
  (get props "infosquito.beanstalk.tube"))
  

(defn get-es-scroll-page-size
  [props]
  (get-int-prop props "infosquito.es.scroll-page-size"))


(defn get-es-scroll-ttl
  [props]
  (get props "infosquito.es.scroll-ttl"))

  
(defn get-es-url
  [props]
  (URL. "http"
        (get props "infosquito.es.host")
        (get-int-prop props "infosquito.es.port")
        ""))


(defn get-irods-default-resource
  [props]
  (get props "infosquito.irods.default-resource"))  


(defn get-irods-home
  [props]
  (get props "infosquito.irods.home"))


(defn get-irods-host
  [props]
  (get props "infosquito.irods.host"))
  

(defn get-irods-index-root
  [props]
  (get props "infosquito.irods.index-root"))

  
(defn get-irods-password
  [props]
  (get props "infosquito.irods.password"))


(defn get-irods-port
  [props]
  (get-int-prop props "infosquito.irods.port"))


(defn get-irods-user
  [props]
  (get props "infosquito.irods.user"))


(defn get-irods-zone
  [props]
  (get props "infosquito.irods.zone"))


(defn get-retry-delay
  [props]
  (get-int-prop props "infosquito.retry-delay"))


(defn validate
  [props log-invalid]
  (let [exists? (fn [label] (if (get props label)
                              true
                              (do
                                (log-invalid "The property" label 
                                             "is missing from the configuration.")
                                false)))     
        labels ["infosquito.beanstalk.host"
                "infosquito.beanstalk.port"
                "infosquito.beanstalk.job-ttr"
                "infosquito.beanstalk.tube"
                "infosquito.es.host"
                "infosquito.es.port"
                "infosquito.es.scroll-ttl"
                "infosquito.es.scroll-page-size"
                "infosquito.irods.host"
                "infosquito.irods.port"
                "infosquito.irods.user"
                "infosquito.irods.password"
                "infosquito.irods.home"
                "infosquito.irods.zone"
                "infosquito.irods.default-resource"
                "infosquito.irods.index-root"
                "infosquito.retry-delay"]]
    (every? exists? labels)))
