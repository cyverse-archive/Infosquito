(defproject infosquito "0.1.0-SNAPSHOT"
  :description "This is a web service to enable searching of iPlant 
                Collaborative user content."
  :license {:url "file://LICENSE.txt"}
  :aot [infosquito.core]
  :main infosquito.core
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/data.json "0.1.2"]
                 [org.clojure/tools.cli "0.2.1"]
                 [org.clojure/tools.logging "0.2.3"]
                 [clojurewerkz/elastisch "1.0.0"]
                 [com.github.drsnyder/beanstalk "1.0.0-clj14"]
                 [slingshot "0.10.3"]
                 [org.iplantc/clj-jargon "0.2.2-SNAPSHOT"]
                 [org.iplantc/clojure-commons "1.2.1-SNAPSHOT"]]
  :plugins [[org.iplantc/lein-iplant-rpm "1.3.0-SNAPSHOT"]]
  :iplant-rpm {:summary      "infosquito"
               :dependencies ["iplant-service-config >= 0.1.0-5"]
               :config-files ["log4j.properties"]
               :config-path  "resources"}
  :repositories {"iplantCollaborative"
                 "http://projects.iplantcollaborative.org/archiva/repository/internal/"
                 
                 "sonatype"
                 "http://oss.sonatype.org/content/repositories/releases/"})
