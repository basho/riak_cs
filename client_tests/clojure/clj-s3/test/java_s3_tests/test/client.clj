;; Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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
           com.amazonaws.services.s3.model.AmazonS3Exception)

  (:require [aws.sdk.s3 :as s3])
  (:require [java-s3-tests.user-creation :as user-creation])
  (:require [clojure.tools.logging :as log])
  (:use midje.sweet))

(def ^:internal riak-cs-host "127.0.0.1")
(def ^:internal riak-cs-host-with-protocol (str "http://" riak-cs-host))

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

(defn user-to-cred
  [user]
  {:endpoint "http://s3.amazonaws.com"
   :access-key (:key_id user)
   :secret-key (:key_secret user)
   :proxy {:host riak-cs-host
           :port (get-riak-cs-port)}})

(defn random-user
  []
  (user-creation/create-random-user riak-cs-host-with-protocol (get-riak-cs-port)))

(defmacro with-random-user
  "Execute `body` in implicit do with a random-user bound to `var-name`"
  [var-name & body]
  `(let [~var-name (random-user)]
     (do ~@body)))

(defn random-cred
  []
  (user-to-cred (random-user)))

(defmacro with-random-cred
  "Execute `body` in implicit do with a random-cred bound to `var-name`"
  [var-name & body]
  `(let [~var-name (random-cred)]
     (do ~@body)))

(defn random-string []
  (str (java.util.UUID/randomUUID)))

(defn write-file [filename content]
  (with-open [w (clojure.java.io/writer filename :append false)]
    (.write w content)))

(defn etag-suffix [etag]
  (subs etag (- (count etag) 2)))

(defn upload-file [cred bucket key file-name part-size]
  (let [f (clojure.java.io/file file-name)]
    (s3/put-multipart-object cred bucket key f {:part-size part-size :threads 2})
    (.delete f)))

(fact "bogus creds raises an exception"
      (let [bogus-user {:key_id "foo"
                        :key_secret "bar"}]
        (s3/list-buckets (user-to-cred bogus-user)))
      => (throws AmazonS3Exception))

(fact "new users have no buckets"
      (with-random-cred c
        (s3/list-buckets c))
      => [])

(let [bucket-name (random-string)]
  (fact "creating a bucket should list one bucket in list buckets"
        (with-random-cred c
          (s3/create-bucket c bucket-name)
          (map :name (s3/list-buckets c))
        => [bucket-name])))

(let [bucket-name (random-string)
      object-name (random-string)]
  (fact "simple put works"
        (with-random-cred cred
          (s3/create-bucket cred bucket-name)
          (s3/put-object cred bucket-name object-name
                         "contents"))
        => truthy))

(let [bucket-name (random-string)
      object-name (random-string)
      value "this is the value!"]
  (fact "the value received during GET is the same
         as the object that was PUT"
        (with-random-cred c
          (s3/create-bucket c bucket-name)
          (s3/put-object c bucket-name object-name value)
          ((comp slurp :content) (s3/get-object c bucket-name object-name)))
        => value))

(let [bucket-name (random-string)
      object-name (random-string)
      value "this is the value!"
      as-bytes (.getBytes value "UTF-8")
      md5-sum (md5-string as-bytes)]
  (fact "check that the etag of the response is the same as the md5
         of the original object"
        (with-random-cred c
          (s3/create-bucket c bucket-name)
          (s3/put-object c bucket-name object-name value)
          ((comp :etag :metadata)
           (s3/get-object
            c bucket-name object-name)))
        => md5-sum))

(let [bucket-name (random-string)
      object-name (random-string)
      value "aaaaaaaaaabbbbbbbbbb"
      file-name "./clj-mp-test.txt"]
  (fact "multipart upload works"
        (with-random-cred c
          (s3/create-bucket c bucket-name)
          (write-file file-name value)
          (upload-file c bucket-name object-name file-name 10)
          (let [fetched-object (s3/get-object
                                c bucket-name object-name)]
            ((comp slurp :content) fetched-object)
            => value
            ((comp etag-suffix :etag :metadata) fetched-object)
            => "-2"))))

(let [bucket-name (random-string)
      object-name (random-string)
      value "this is the real value"
      wrong-md5 "2945d7de2f70de5b8c0cb3fbcba4fe92"]
  (fact "Bad content md5 throws an exception"
        (with-random-cred c
          (s3/create-bucket c bucket-name)
          (s3/put-object c bucket-name object-name value
                         {:content-md5 wrong-md5}))
        => (throws AmazonS3Exception)))

(def bad-canonical-id
  "0f80b2d002a3d018faaa4a956ce8aa243332a30e878f5dc94f82749984ebb30b")

(let [bucket-name (random-string)
      object-name (random-string)
      value-string "this is the real value"]
  (fact "Nonexistent canonical-id grant header returns HTTP 400 on
        a put object request (not just an ACL subresource request)"
        (with-random-cred c
          (s3/create-bucket c bucket-name)
          (s3/put-object c bucket-name object-name value-string {}
                         (s3/grant {:id bad-canonical-id} :full-control)))
        => (throws AmazonS3Exception)))

(let [bucket-name (random-string)
      object-name (random-string)
      value-string "this is the real value"
      public-read-grant {:grantee :all-users, :permission :read}]
  (fact "Creating an object with an ACL returns the same ACL when you read
        the ACL"
        (with-random-cred c
          (s3/create-bucket c bucket-name)
          (s3/put-object c bucket-name object-name value-string {}
                         (s3/grant :all-users :read))
          (:grants (s3/get-object-acl c bucket-name object-name)))
        => (contains (just public-read-grant))))

(let [bucket-name (random-string)
      object-name (random-string)
      value-string "this is the real value"
      public-read-grant {:grantee :all-users, :permission :read}]
  (fact "Creating an object with an ACL returns the same ACL when you read
        the ACL"
        (with-random-cred c
          (s3/create-bucket c bucket-name)
          (s3/put-object c bucket-name object-name value-string {}
                         (s3/grant :all-users :read))
          (:grants (s3/get-object-acl c bucket-name object-name)))
        => (contains (just public-read-grant))))

(let [bucket-name (random-string)
      object-name (random-string)
      value-string "this is the real value"]
  (with-random-cred c
    (with-random-user u2
      (fact "Creating an object with an (non-canned) ACL returns the same ACL
             when you read the ACL"
            (s3/create-bucket c bucket-name)
            (s3/put-object c bucket-name object-name value-string {}
                           (s3/grant {:id (:id u2)} :read))
            (:grants (s3/get-object-acl c bucket-name object-name))
            => (contains (just {:grantee {:id (:id u2),
                                          :display-name (:display_name u2)},
                                :permission :read}))))))

(let [bucket-name (random-string)
      object-name (random-string)]
  (fact "Creating a bucket with an ACL returns the same ACL when you read
        the ACL"
        (with-random-cred c
          (do
            (s3/create-bucket c bucket-name {}
                              (s3/grant :all-users :read))
            (:grants (s3/get-bucket-acl c bucket-name))))
        => (contains (just {:grantee :all-users, :permission :read}))))

(let [bucket-name (random-string)
      object-name (random-string)]
  (with-random-cred c
    (with-random-user u2
        (fact "Creating a bucket with an (non-canned) ACL returns the same ACL
               when you read the ACL"
               (s3/create-bucket c bucket-name {}
                                 (s3/grant {:id (:id u2)} :write))
               (:grants (s3/get-bucket-acl c bucket-name))
               => (contains (just {:grantee {:id (:id u2),
                                             :display-name (:display_name u2)},
                                   :permission :write}))))))
