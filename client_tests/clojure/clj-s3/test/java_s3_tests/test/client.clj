;; Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
;;
;; This file is provided to you under the Apache License,
;; Version 2.0 (the "License"); you may not use this file
;; except in compliance with the License.  You may obtain
;; a copy of the License at
;;
;;   http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing,
;; software distributed under the License is distributed on an
;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
;; KIND, either express or implied.  See the License for the
;; specific language governing permissions and limitations
;; under the License.

(ns java-s3-tests.test.client
  (:import java.security.MessageDigest
           org.apache.commons.codec.binary.Hex
           com.amazonaws.services.s3.model.AmazonS3Exception
           com.amazonaws.services.s3.model.ObjectMetadata
           com.amazonaws.services.s3.transfer.TransferManager
           com.amazonaws.services.s3.transfer.TransferManagerConfiguration)

  (:require [aws.sdk.s3 :as s3])
  (:require [java-s3-tests.user-creation :as user-creation])
  (:use midje.sweet))

(def ^:internal riak-cs-host-with-protocol "http://localhost")
(def ^:internal riak-cs-host "localhost")

(defn get-riak-cs-port-str
  "Try to get a TCP port number from the OS environment"
  []
  (let [port-str (get (System/getenv) "CS_HTTP_PORT")]
       (cond (nil? port-str) "8080"
             :else           port-str)))

(defn get-riak-cs-port []
  (Integer/parseInt (get-riak-cs-port-str) 10))

(defn md5-byte-array [input-byte-array]
  (let [instance (MessageDigest/getInstance "MD5")]
    (.digest instance input-byte-array)))

(defn md5-string
  [input-byte-array]
  (let [b (md5-byte-array input-byte-array)]
    (String. (Hex/encodeHex b))))

(defn random-client
  []
  (let [new-creds (user-creation/create-random-user
                    riak-cs-host-with-protocol
                    (get-riak-cs-port))]
    (s3/client (:key_id new-creds)
               (:key_secret new-creds)
               {:proxy-host riak-cs-host
                :proxy-port (get-riak-cs-port)
                :protocol :http})))

(defmacro with-random-client
  "Execute `form` with a random-client
  bound to `var-name`"
  [var-name form]
  `(let [~var-name (random-client)]
     ~form))

(defn random-string []
  (str (java.util.UUID/randomUUID)))

(defn write-file [filename content]
  (with-open [w (clojure.java.io/writer  filename :append false)]
    (.write w content)))

(defn etag-suffix [etag]
  (subs etag (- (count etag) 2)))

(defn create-manager [c]
  (TransferManager. c))

(defn configure-manager [tm]
  (let [tm-config (.getConfiguration tm)]
    (.setMultipartUploadThreshold tm-config 19)
    (.setMinimumUploadPartSize tm-config 10)
    (.setConfiguration tm tm-config)))

(defn create-and-configure-manager [c]
  (let [tm (create-manager c)]
    (configure-manager tm)
    tm))

(defn upload-file [tm bucket-name object-name file-name]
  (let [f (clojure.java.io/file file-name)
        u (.upload tm bucket-name object-name f)]
    (.waitForCompletion u)
    (.delete f)))

(fact "bogus creds raises an exception"
      (let [bogus-client
            (s3/client "foo"
                       "bar"
                       {:endpoint (str "http://localhost:"
                                       (get-riak-cs-port-str))})]
        (s3/list-buckets bogus-client))
      => (throws AmazonS3Exception))

(fact "new users have no buckets"
        (with-random-client c
          (s3/list-buckets c))
        => [])

(let [bucket-name (random-string)]
  (fact "creating a bucket should list
        one bucket in list buckets"
        (with-random-client c
          (do (s3/create-bucket c bucket-name)
            ((comp :name first) (s3/list-buckets c))))
        => bucket-name))

(let [bucket-name (random-string)
      object-name (random-string)]
  (fact "simple put works"
        (with-random-client c
          (do (s3/create-bucket c bucket-name)
            (s3/put-object c bucket-name object-name
                           "contents")))
        => truthy))

(let [bucket-name (random-string)
      object-name (random-string)
      value "this is the value!"]
  (fact "the value received during GET is the same
        as the object that was PUT"
        (with-random-client c
          (do (s3/create-bucket c bucket-name)
            (s3/put-object c bucket-name object-name
                           value)
            ((comp slurp :content) (s3/get-object c bucket-name object-name))))
        => value))

(let [bucket-name (random-string)
      object-name (random-string)
      value "this is the value!"
      as-bytes (.getBytes value "UTF-8")
      md5-sum (md5-string as-bytes)]
  (fact "check that the etag of the response
        is the same as the md5 of the original
        object"
        (with-random-client c
          (do (s3/create-bucket c bucket-name)
            (s3/put-object c bucket-name object-name
                           value)
            ((comp :etag :metadata)
               (s3/get-object
                 c bucket-name object-name))))
        => md5-sum))

(let [bucket-name (random-string)
      object-name (random-string)
      value (str "aaaaaaaaaa" "bbbbbbbbbb")
      file-name "./clj-mp-test.txt"]
  (fact "mulitpart upload works"
        (with-random-client c
          (do
            (s3/create-bucket c bucket-name)
            (let [tm (create-and-configure-manager c)]
              (write-file file-name value)
              (upload-file tm bucket-name object-name file-name)
              (let [fetched-object (s3/get-object
                                      c bucket-name object-name)]
                  [((comp slurp :content) fetched-object)
                   ((comp etag-suffix :etag :metadata) fetched-object)]))))
        => [value, "-2"]))
