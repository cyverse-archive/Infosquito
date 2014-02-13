(defproject org.iplantc/infosquito "1.8.6-SNAPSHOT"
  :description "An ICAT database crawler used to index the contents of iRODS."
  :url "http://www.iplantcollaborative.org"
  :license {:name "BSD"
            :url "http://iplantcollaborative.org/sites/default/files/iPLANT-LICENSE.txt"}
  :scm {:connection "scm:git:git@github.com:iPlantCollaborativeOpenSource/facepalm.git"
        :developerConnection "scm:git:git@github.com:iPlantCollaborativeOpenSource/facepalm.git"
        :url "git@github.com:iPlantCollaborativeOpenSource/facepalm.git"}
  :pom-addition [:developers
                 [:developer
                  [:url "https://github.com/orgs/iPlantCollaborativeOpenSource/teams/iplant-devs"]]]
  :aot [infosquito.core]
  :main infosquito.core
  :classifiers [["javadoc" :javadoc]
                ["sources" :sources]]
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
                 [org.iplantc/clojure-commons "1.4.8-SNAPSHOT"]]
  :profiles {:dev {:resource-paths ["dev-resources"]}}
  :plugins [[org.iplantc/lein-iplant-rpm "1.4.3-SNAPSHOT"]]
  :iplant-rpm {:summary      "infosquito"
               :dependencies ["iplant-service-config >= 0.1.0-5"]
               :config-files ["log4j.properties"]
               :config-path  "conf"}
  :repositories [["sonatype-nexus-snapshots"
                  {:url "https://oss.sonatype.org/content/repositories/snapshots"}]]
  :deploy-repositories [["sonatype-nexus-staging"
                         {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2/"}]
                        ["renci.repository"
                         {:url "http://ci-dev.renci.org/nexus/content/repositories/releases/"}]])
