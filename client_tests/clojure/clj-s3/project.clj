(defproject java-s3-tests/java-s3-tests "0.0.1"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.reiddraper/clj-aws-s3 "0.4.2"]
                 [clj-http "0.7.1"]
                 [cheshire "5.1.0"]]
  :profiles {:dev {:dependencies [[midje "1.5.1"]]}}
  :min-lein-version "2.0.0"
  :plugins [[lein-midje "3.0.1"]]
  :description "integration tests for Riak CS using the offical Java SDK")
