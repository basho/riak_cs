(defproject java-s3-tests/java-s3-tests "0.0.1-SNAPSHOT" 
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.amazonaws/aws-java-sdk "1.3.11"]
                 [com.reiddraper/clj-aws-s3 "0.4.0-SNAPSHOT"]
                 [commons-codec/commons-codec "1.6"]
                 [clj-http "0.4.3"]
                 [cheshire "4.0.0"]]
  :profiles {:dev {:dependencies [[midje "1.4.0"]]}}
  :min-lein-version "2.0.0"
  :plugins [[lein-midje "2.0.0-SNAPSHOT"]]
  :description "integration tests for Riak CS using the offical Java SDK")
