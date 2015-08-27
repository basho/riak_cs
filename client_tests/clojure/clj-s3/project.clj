(defproject java-s3-tests/java-s3-tests "0.0.2"
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 ;; aws-sdk-java deps
                 [com.amazonaws/aws-java-sdk-s3 "1.10.8" :exclusions [joda-time]]
                 [joda-time "2.8.1"]
                 [com.fasterxml.jackson.core/jackson-core "2.5.3"]
                 [com.fasterxml.jackson.core/jackson-databind "2.5.3"]
                 ;; user_creation deps
                 [clj-http "2.0.0"]
                 [cheshire "5.1.0"]]
  ;; :jvm-opts ["-verbose:class"]
  :profiles {:dev {:dependencies [[midje "1.7.0"]]}}
  :min-lein-version "2.0.0"
  :plugins [[lein-midje "3.1.3"]]
  :description "integration tests for Riak CS using the offical Java SDK")
