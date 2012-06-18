(defproject java-s3-tests "0.0.1-SNAPSHOT"
  :description "integration tests for Riak CS using the offical Java SDK"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.amazonaws/aws-java-sdk "1.3.11"]
                 [com.reiddraper/clj-aws-s3 "0.4.0-SNAPSHOT"]
                 [commons-codec/commons-codec "1.6"]
                 [clj-http "0.4.3"]
                 [cheshire "4.0.0"]]
  :dev-dependencies [[midje "1.4.0"]
                    [lein-midje "1.0.9"]])
