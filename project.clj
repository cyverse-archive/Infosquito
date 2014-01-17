(defproject infosquito "1.8.4-SNAPSHOT"
  :description "This is a web service to enable searching of iPlant Collaborative user content."
  :license {:url "file://LICENSE.txt"}
  :aot [infosquito.core]
  :main infosquito.core
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [org.clojure/tools.cli "0.2.4"]
                 [org.clojure/tools.logging "0.2.6"]
                 [cheshire "5.2.0"]
                 [clj-time "0.6.0"]
                 [clojurewerkz/elastisch "1.3.0"]
                 [com.novemberain/langohr "2.1.0"]
                 [postgresql "9.0-801.jdbc4"]
                 [slingshot "0.10.3"]
                 [org.iplantc/clj-jargon "0.4.0"]
                 [org.iplantc/clojure-commons "1.4.7"]]
  :profiles {:dev {:dependencies   [[org.iplantc/boxy "0.1.2-SNAPSHOT"]]
                   :resource-paths ["dev-resources"]}}
  :plugins [[org.iplantc/lein-iplant-rpm "1.4.3-SNAPSHOT"]]
  :iplant-rpm {:summary      "infosquito"
               :dependencies ["iplant-service-config >= 0.1.0-5"]
               :config-files ["log4j.properties"]
               :config-path  "conf"}
  :repositories {"iplantCollaborative"
                 "http://projects.iplantcollaborative.org/archiva/repository/internal/"

                 "sonatype"
                 "http://oss.sonatype.org/content/repositories/releases/"

                 "renci.repository"
                 "http://ci-dev.renci.org/nexus/content/repositories/releases/"})
