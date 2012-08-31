(defproject infosquito "0.1.0-SNAPSHOT"
  :description "This is a web service to enable searching of iPlant 
                Collaborative user content."
  :license {:url "file://LICENSE.txt"}
  :aot [infosquito.core]
  :main infosquito.core
  :dependencies [[org.iplantc/clojure-commons "1.2.0-SNAPSHOT"]
                 [org.clojure/clojure "1.4.0"]
                 [org.clojure/data.json "0.1.2"]
                 [org.clojure/tools.cli "0.2.1"]
                 [org.clojure/tools.logging "0.2.3"]
                 [compojure "1.1.1"]
                 [org.elasticsearch/elasticsearch "0.19.8"]
                 [ring "1.1.2"]]
  :profiles {:dev {:dependencies [[ring-mock "0.1.3"]]}}
  :plugins [[org.iplantc/lein-iplant-rpm "1.3.0-SNAPSHOT"]]
  :iplant-rpm {:summary      "infosquito"
               :dependencies ["iplant-service-config >= 0.1.0-5"]
               :config-files ["log4j.properties"]
               :config-path  "resources"}
  :repositories {"iplantCollaborative"
                 "http://projects.iplantcollaborative.org/archiva/repository/internal/"
                 
                 "sonatype"
                 "http://oss.sonatype.org/content/repositories/releases/"})
