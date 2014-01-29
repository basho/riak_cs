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

(ns java-s3-tests.user-creation
  (:require [cheshire.core :as cheshire])
  (:require [clj-http.client :as http]))

(defn ^:internal make-host-port-string [host port]
  (str host ":" port))

(defn ^:internal make-url-with-resource [host-port-string resource]
  (str host-port-string "/" resource))

(defn ^:internal make-body [user-name email]
  (cheshire/generate-string
    {"email" email "name" user-name}))

(defn ^:internal make-user-url [host port]
  (let [location (make-host-port-string host port)]
    (make-url-with-resource location "/riak-cs/user")))

(defn ^:internal parse-response-body [string]
  (cheshire/parse-string string true))

(defn ^:internal parse-response [response]
  (parse-response-body (:body response)))

(defn ^:internal string-uuid []
  (str (java.util.UUID/randomUUID)))

(defn create-user
  "create a new user from the /user
  resource. Returns a map with keys
  :key_id and :key_secret"
  [host port user-name email]
  (let [url (make-user-url host port)
        body (make-body user-name email)
        headers {"Content-Type" "application/json"}]
    (parse-response (http/post url {:body body :headers headers}))))

(defn create-random-user [host port]
  (let [random-token (string-uuid)
        user-name random-token
        email (str random-token "@example.com")]
    (create-user host port user-name email)))
