(defproject infosquito "0.2.1-SNAPSHOT"
  :description "This is a web service to enable searching of iPlant Collaborative user content."
  :license {:url "file://LICENSE.txt"}
  :aot [infosquito.core]
  :main infosquito.core
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [org.clojure/tools.logging "0.2.6"]
                 [cheshire "5.1.0"]
                 [clojurewerkz/elastisch "1.0.2"]
                 [com.github.drsnyder/beanstalk "1.0.0-clj14"]
                 [slingshot "0.10.3"]
                 [org.iplantc/clj-jargon "0.2.5-SNAPSHOT"]
                 [org.iplantc/clojure-commons "1.4.1-SNAPSHOT"]]
  :profiles {:dev {:dependencies   [[org.iplantc/boxy "0.1.2-SNAPSHOT"]]
                   :resource-paths ["dev-resources"]}}
  :plugins [[org.iplantc/lein-iplant-rpm "1.4.1-SNAPSHOT"]]
  :iplant-rpm {:summary      "infosquito"
               :dependencies ["iplant-service-config >= 0.1.0-5"]
               :config-files ["log4j.properties"]
               :config-path  "resources"}
  :repositories {"iplantCollaborative"
                 "http://projects.iplantcollaborative.org/archiva/repository/internal/"

                 "sonatype"
                 "http://oss.sonatype.org/content/repositories/releases/"

                 "renci.repository"
                 "http://ci-dev.renci.org/nexus/content/repositories/releases/"})
