(ns infosquito.props-test
  (:use clojure.test
        infosquito.props)
  (:import [java.io FileReader] 
           [java.util Properties]))


(def ^{:private true} props
  (doto (Properties.)
    (.load (FileReader. "dev-resources/local.properties"))))


(deftest test-get-beanstalk-host
  (is (= "localhost" (get-beanstalk-host props))))


(deftest test-get-job-ttr
  (is (= 120 (get-job-ttr props))))


(deftest test-get-beanstalk-port
  (is (= 11300 (get-beanstalk-port props))))


(deftest test-get-work-tube
  (is (= "infosquito" (get-work-tube props))))


(deftest test-get-es-scroll-page-size
  (is (= 50 (get-es-scroll-page-size props))))


(deftest test-get-es-scroll-ttl
  (is (= "10m" (get-es-scroll-ttl props))))


(deftest test-get-es-url
  (is (= "http://localhost:9200" 
         (str (get-es-url props)))))


(deftest test-get-irods-default-resource
  (is (= "demoResc" (get-irods-default-resource props))))


(deftest test-get-irods-home
  (is (= "/tempZone/home/rods" (get-irods-home props))))


(deftest test-get-irods-host
  (is (= "localhost" (get-irods-host props))))


(deftest test-get-irods-index-root
  (is (= "/tempZone/home" (get-irods-index-root props))))


(deftest test-get-irods-password
  (is (= "rods" (get-irods-password props))))


(deftest test-get-irods-port
  (is (= 1247 (get-irods-port props))))


(deftest test-get-irods-user
  (is (= "rods" (get-irods-user props))))


(deftest test-get-irods-zone
  (is (= "tempZone" (get-irods-zone props))))


(deftest test-get-retry-delay
  (is (= 10000 (get-retry-delay props))))

        
(deftest test-validate
  (testing "all properties to be validated are in local.properties"
    (is (validate props println)))
  (testing "all properties in local.properties are being validated"
   (letfn [(logs-error [name & msg] 
                       (is (= (str "The property " name 
                                   " is missing from the configuration.")
                              (apply print-str msg))))]
     (doseq [name (keys props)]
       (let [props' (.clone props)]
         (.remove props' name)
         (is (false? (validate props' (partial logs-error name)))))))))
  
