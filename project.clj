(defproject infosquito "0.1.0-SNAPSHOT"
  :description "This is a web service to enable searching of iPlant 
                Collaborative user content."
  :license {:url "file://LICENSE.txt"}
  :aot [infosquito.core]
  :main infosquito.core
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/data.json "0.1.2"]
                 [org.clojure/tools.logging "0.2.3"]
                 [compojure "1.1.1"]
                 [ring "1.1.2"]
                 ; test dependencies below here
                 [ring-mock "0.1.3"]])

; TODO Find out how to have test-specific dependencies.  I don't want the 
; release to depend on ring-mock.
