(ns aws.sdk.s3
  "Functions to access the Amazon S3 storage service.

  Each function takes a map of credentials as its first argument. The
  credentials map should contain an :access-key key and a :secret-key key,
  optionally an :endpoint key to denote an AWS endpoint and optionally a :proxy
  key to define a HTTP proxy to go through.

  The :proxy key must contain keys for :host and :port, and may contain keys
  for :user, :password, :domain and :workstation."
  (:require [clojure.string :as str]
            [clj-time.core :as t]
            [clj-time.coerce :as coerce]
            [clojure.walk :as walk])
  (:import com.amazonaws.auth.BasicAWSCredentials
           com.amazonaws.auth.BasicSessionCredentials
           com.amazonaws.services.s3.AmazonS3Client
           com.amazonaws.AmazonServiceException
           com.amazonaws.ClientConfiguration
           com.amazonaws.HttpMethod
           com.amazonaws.services.s3.model.AccessControlList
           com.amazonaws.services.s3.model.Bucket
           com.amazonaws.services.s3.model.Grant
           com.amazonaws.services.s3.model.CanonicalGrantee
           com.amazonaws.services.s3.model.CopyObjectResult
           com.amazonaws.services.s3.model.EmailAddressGrantee
           com.amazonaws.services.s3.model.GetObjectRequest
           com.amazonaws.services.s3.model.GetObjectMetadataRequest
           com.amazonaws.services.s3.model.Grant
           com.amazonaws.services.s3.model.GroupGrantee
           com.amazonaws.services.s3.model.ListObjectsRequest
           com.amazonaws.services.s3.model.ListVersionsRequest
           com.amazonaws.services.s3.model.Owner
           com.amazonaws.services.s3.model.ObjectMetadata
           com.amazonaws.services.s3.model.ObjectListing
           com.amazonaws.services.s3.model.Permission
           com.amazonaws.services.s3.model.PutObjectRequest
           com.amazonaws.services.s3.model.S3Object
           com.amazonaws.services.s3.model.S3ObjectSummary
           com.amazonaws.services.s3.model.S3VersionSummary
           com.amazonaws.services.s3.model.VersionListing
           com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
           com.amazonaws.services.s3.model.AbortMultipartUploadRequest
           com.amazonaws.services.s3.model.CompleteMultipartUploadRequest
           com.amazonaws.services.s3.model.UploadPartRequest
           java.util.concurrent.Executors
           java.io.ByteArrayInputStream
           java.io.File
           java.io.InputStream
           java.nio.charset.Charset))

(defn- s3-client*
  [cred]
  (let [client-configuration (ClientConfiguration.)]
    (when-let [conn-timeout (:conn-timeout cred)]
      (.setConnectionTimeout client-configuration conn-timeout))
    (when-let [socket-timeout (:socket-timeout cred)]
      (.setSocketTimeout client-configuration socket-timeout))
    (when-let [max-retries (:max-retries cred)]
      (.setMaxErrorRetry client-configuration max-retries))
    (when-let [max-conns (:max-conns cred)]
      (.setMaxConnections client-configuration max-conns))
    (when-let [proxy-host (get-in cred [:proxy :host])]
      (.setProxyHost client-configuration proxy-host))
    (when-let [proxy-port (get-in cred [:proxy :port])]
      (.setProxyPort client-configuration proxy-port))
    (when-let [proxy-user (get-in cred [:proxy :user])]
      (.setProxyUsername client-configuration proxy-user))
    (when-let [proxy-pass (get-in cred [:proxy :password])]
      (.setProxyPassword client-configuration proxy-pass))
    (when-let [proxy-domain (get-in cred [:proxy :domain])]
      (.setProxyDomain client-configuration proxy-domain))
    (when-let [proxy-workstation (get-in cred [:proxy :workstation])]
      (.setProxyWorkstation client-configuration proxy-workstation))
    (let [aws-creds
          (if (:token cred)
            (BasicSessionCredentials. (:access-key cred) (:secret-key cred) (:token cred))
            (BasicAWSCredentials. (:access-key cred) (:secret-key cred)))

          client (AmazonS3Client. aws-creds client-configuration)]
      (when-let [endpoint (:endpoint cred)]
        (.setEndpoint client endpoint))
      client)))

(def ^{:private true :tag AmazonS3Client}
  s3-client
  (memoize s3-client*))

(defprotocol ^{:no-doc true} Mappable
  "Convert a value into a Clojure map."
  (^{:no-doc true} to-map [x] "Return a map of the value."))

(extend-protocol Mappable
  Bucket
  (to-map [bucket]
    {:name          (.getName bucket)
     :creation-date (.getCreationDate bucket)
     :owner         (to-map (.getOwner bucket))})
  Owner
  (to-map [owner]
    {:id           (.getId owner)
     :display-name (.getDisplayName owner)})
  nil
  (to-map [_] nil))

(defn bucket-exists?
  "Returns true if the supplied bucket name already exists in S3."
  [cred name]
  (.doesBucketExist (s3-client cred) name))

(defn create-bucket
  "Create a new S3 bucket with the supplied name."
  [cred ^String name]
  (to-map (.createBucket (s3-client cred) name)))

(defn delete-bucket
  "Delete the S3 bucket with the supplied name."
  [cred ^String name]
  (.deleteBucket (s3-client cred) name))

(defn list-buckets
  "List all the S3 buckets for the supplied credentials. The buckets will be
  returned as a seq of maps with the following keys:
    :name          - the bucket name
    :creation-date - the date when the bucket was created
    :owner         - the owner of the bucket"
  [cred]
  (map to-map (.listBuckets (s3-client cred))))

(defprotocol ^{:no-doc true} ToPutRequest
  "A protocol for constructing a map that represents an S3 put request."
  (^{:no-doc true} put-request [x] "Convert a value into a put request."))

(extend-protocol ToPutRequest
  InputStream
  (put-request [is] {:input-stream is})
  File
  (put-request [f] {:file f})
  String
  (put-request [s]
    (let [bytes (.getBytes s)]
      {:input-stream     (ByteArrayInputStream. bytes)
       :content-length   (count bytes)
       :content-type     (str "text/plain; charset=" (.name (Charset/defaultCharset)))})))

(defmacro set-attr
  "Set an attribute on an object if not nil."
  {:private true}
  [object setter value]
  `(if-let [v# ~value]
     (~setter ~object v#)))

(defn- maybe-int [x]
  (if x (int x)))

(defn- map->ObjectMetadata
  "Convert a map of object metadata into a ObjectMetadata instance."
  [metadata]
  (doto (ObjectMetadata.)
    (set-attr .setCacheControl         (:cache-control metadata))
    (set-attr .setContentDisposition   (:content-disposition metadata))
    (set-attr .setContentEncoding      (:content-encoding metadata))
    (set-attr .setContentLength        (:content-length metadata))
    (set-attr .setContentMD5           (:content-md5 metadata))
    (set-attr .setContentType          (:content-type metadata))
    (set-attr .setServerSideEncryption (:server-side-encryption metadata))
    (set-attr .setUserMetadata
              (walk/stringify-keys (dissoc metadata
                                           :cache-control
                                           :content-disposition
                                           :content-encoding
                                           :content-length
                                           :content-md5
                                           :content-type
                                           :server-side-encryption)))))

(defn- ^PutObjectRequest ->PutObjectRequest
  "Create a PutObjectRequest instance from a bucket name, key and put request
  map."
  [^String bucket ^String key request]
  (cond
   (:file request)
     (let [put-obj-req (PutObjectRequest. bucket key ^java.io.File (:file request))]
       (.setMetadata put-obj-req (map->ObjectMetadata (dissoc request :file)))
       put-obj-req)
   (:input-stream request)
     (PutObjectRequest.
      bucket key
      (:input-stream request)
      (map->ObjectMetadata (dissoc request :input-stream)))))

(declare create-acl) ; used by put-object

(defn put-object
  "Put a value into an S3 bucket at the specified key. The value can be
  a String, InputStream or File (or anything that implements the ToPutRequest
  protocol).

  An optional map of metadata may also be supplied that can include any of the
  following keys:
    :cache-control          - the cache-control header (see RFC 2616)
    :content-disposition    - how the content should be downloaded by browsers
    :content-encoding       - the encoding of the content (e.g. gzip)
    :content-length         - the length of the content in bytes
    :content-md5            - the MD5 sum of the content
    :content-type           - the mime type of the content
    :server-side-encryption - set to AES256 if SSE is required

  An optional list of grant functions can be provided after metadata.
  These functions will be applied to a clear ACL and the result will be
  the ACL for the newly created object."
  [cred bucket key value & [metadata & permissions]]
  (let [req (->> (merge (put-request value) metadata)
                 (->PutObjectRequest bucket key))]
    (when permissions
      (.setAccessControlList req (create-acl permissions)))
    (.putObject (s3-client cred) req)))

(defn- initiate-multipart-upload
  [cred bucket key] 
  (.getUploadId (.initiateMultipartUpload 
                  (s3-client cred) 
                  (InitiateMultipartUploadRequest. bucket key))))

(defn- abort-multipart-upload
  [{cred :cred bucket :bucket key :key upload-id :upload-id}] 
  (.abortMultipartUpload 
    (s3-client cred) 
    (AbortMultipartUploadRequest. bucket key upload-id)))

(defn- complete-multipart-upload
  [{cred :cred bucket :bucket key :key upload-id :upload-id e-tags :e-tags}] 
  (.completeMultipartUpload
    (s3-client cred)
    (CompleteMultipartUploadRequest. bucket key upload-id e-tags)))

(defn- upload-part
  [{cred :cred bucket :bucket key :key upload-id :upload-id
    part-size :part-size offset :offset ^java.io.File file :file}] 
  (.getPartETag
   (.uploadPart
    (s3-client cred)
    (doto (UploadPartRequest.)
      (.setBucketName bucket)
      (.setKey key)
      (.setUploadId upload-id)
      (.setPartNumber (+ 1 (/ offset part-size)))
      (.setFileOffset offset)
      (.setPartSize ^long (min part-size (- (.length file) offset)))
      (.setFile file)))))

(defn put-multipart-object
  "Do a multipart upload of a file into a S3 bucket at the specified key.
  The value must be a java.io.File object.  The entire file is uploaded 
  or not at all.  If an exception happens at any time the upload is aborted 
  and the exception is rethrown. The size of the parts and the number of
  threads uploading the parts can be configured in the last argument as a
  map with the following keys:
    :part-size - the size in bytes of each part of the file.  Must be 5mb
                 or larger.  Defaults to 5mb
    :threads   - the number of threads that will upload parts concurrently.
                 Defaults to 16."
  [cred bucket key ^java.io.File file & [{:keys [part-size threads]
                            :or {part-size (* 5 1024 1024) threads 16}}]]
  (let [upload-id (initiate-multipart-upload cred bucket key)
        upload    {:upload-id upload-id :cred cred :bucket bucket :key key :file file}
        pool      (Executors/newFixedThreadPool threads)
        offsets   (range 0 (.length file) part-size)
        tasks     (map #(fn [] (upload-part (assoc upload :offset % :part-size part-size)))
                       offsets)]
    (try
      (complete-multipart-upload
        (assoc upload :e-tags (map #(.get ^java.util.concurrent.Future %)  (.invokeAll pool tasks))))
      (catch Exception ex 
        (abort-multipart-upload upload) 
        (.shutdown pool)
        (throw ex))
      (finally (.shutdown pool)))))

(extend-protocol Mappable
  S3Object
  (to-map [object]
    {:content  (.getObjectContent object)
     :metadata (to-map (.getObjectMetadata object))
     :bucket   (.getBucketName object)
     :key      (.getKey object)})
  ObjectMetadata
  (to-map [metadata]
    {:cache-control          (.getCacheControl metadata)
     :content-disposition    (.getContentDisposition metadata)
     :content-encoding       (.getContentEncoding metadata)
     :content-length         (.getContentLength metadata)
     :content-md5            (.getContentMD5 metadata)
     :content-type           (.getContentType metadata)
     :etag                   (.getETag metadata)
     :last-modified          (.getLastModified metadata)
     :server-side-encryption (.getServerSideEncryption metadata)
     :user (walk/keywordize-keys (into {} (.getUserMetadata metadata)))
     :version-id             (.getVersionId metadata)})
  ObjectListing
  (to-map [listing]
    {:bucket          (.getBucketName listing)
     :objects         (map to-map (.getObjectSummaries listing))
     :prefix          (.getPrefix listing)
     :common-prefixes (seq (.getCommonPrefixes listing))
     :truncated?      (.isTruncated listing)
     :max-keys        (.getMaxKeys listing)
     :marker          (.getMarker listing)
     :next-marker     (.getNextMarker listing)})
  S3ObjectSummary
  (to-map [summary]
    {:metadata {:content-length (.getSize summary)
                :etag           (.getETag summary)
                :last-modified  (.getLastModified summary)}
     :bucket   (.getBucketName summary)
     :key      (.getKey summary)})
  S3VersionSummary
  (to-map [summary]
    {:metadata {:content-length (.getSize summary)
                :etag           (.getETag summary)
                :last-modified  (.getLastModified summary)}
     :version-id     (.getVersionId summary)
     :latest?        (.isLatest summary)
     :delete-marker? (.isDeleteMarker summary)
     :bucket         (.getBucketName summary)
     :key            (.getKey summary)})
  VersionListing
  (to-map [listing]
    {:bucket                 (.getBucketName listing)
     :versions               (map to-map (.getVersionSummaries listing))
     :prefix                 (.getPrefix listing)
     :common-prefixes        (seq (.getCommonPrefixes listing))
     :delimiter              (.getDelimiter listing)
     :truncated?             (.isTruncated listing)
     :max-results            (maybe-int (.getMaxKeys listing)) ; AWS API is inconsistent, should be .getMaxResults
     :key-marker             (.getKeyMarker listing)
     :next-key-marker        (.getNextKeyMarker listing)
     :next-version-id-marker (.getNextVersionIdMarker listing)
     :version-id-marker      (.getVersionIdMarker listing)})
  CopyObjectResult
  (to-map [result]
    {:etag                    (.getETag result)
     :expiration-time         (.getExpirationTime result)
     :expiration-time-rule-id (.getExpirationTimeRuleId result)
     :last-modified-date      (.getLastModifiedDate result)
     :server-side-encryption  (.getServerSideEncryption result)}))

(defn get-object
  "Get an object from an S3 bucket. The object is returned as a map with the
  following keys:
    :content  - an InputStream to the content
    :metadata - a map of the object's metadata
    :bucket   - the name of the bucket
    :key      - the object's key
  Be extremely careful when using this method; the :content value in the returned
  map contains a direct stream of data from the HTTP connection. The underlying
  HTTP connection cannot be closed until the user finishes reading the data and
  closes the stream.
  Therefore:
    * Use the data from the :content input stream as soon as possible
    * Close the :content input stream as soon as possible
  If these rules are not followed, the client can run out of resources by
  allocating too many open, but unused, HTTP connections."
  ([cred ^String bucket ^String key]
     (to-map (.getObject (s3-client cred) bucket key)))
  ([cred ^String bucket ^String key ^String version-id]
     (to-map (.getObject (s3-client cred) (GetObjectRequest. bucket key version-id)))))

(defn- map->GetObjectMetadataRequest
  "Create a ListObjectsRequest instance from a map of values."
  [request]
  (GetObjectMetadataRequest. (:bucket request) (:key request) (:version-id request)))

(defn get-object-metadata
  "Get an object's metadata from a bucket.  A optional map of options may be supplied.
   Available options are:
     :version-id - the version of the object
   The metadata is a map with the
   following keys:
     :cache-control          - the CacheControl HTTP header
     :content-disposition    - the ContentDisposition HTTP header
     :content-encoding       - the character encoding of the content
     :content-length         - the length of the content in bytes
     :content-md5            - the MD5 hash of the content
     :content-type           - the mime-type of the content
     :etag                   - the HTTP ETag header
     :last-modified          - the last modified date
     :server-side-encryption - the server-side encryption algorithm"
  [cred bucket key & [options]]
  (to-map
   (.getObjectMetadata
    (s3-client cred)
    (map->GetObjectMetadataRequest (merge {:bucket bucket :key key} options)))))

(defn- map->ListObjectsRequest
  "Create a ListObjectsRequest instance from a map of values."
  ^ListObjectsRequest
  [request]
  (doto (ListObjectsRequest.)
    (set-attr .setBucketName (:bucket request))
    (set-attr .setDelimiter  (:delimiter request))
    (set-attr .setMarker     (:marker request))
    (set-attr .setMaxKeys    (maybe-int (:max-keys request)))
    (set-attr .setPrefix     (:prefix request))))

(defn- http-method [method]
  (-> method name str/upper-case HttpMethod/valueOf))

(defn generate-presigned-url
  "Return a presigned URL for an S3 object. Accepts the following options:
    :expires     - the date at which the URL will expire (defaults to 1 day from now)
    :http-method - the HTTP method for the URL (defaults to :get)"
  [cred bucket key & [options]]
  (.toString
   (.generatePresignedUrl
    (s3-client cred)
    bucket
    key
    (coerce/to-date (:expires options (-> 1 t/days t/from-now)))
    (http-method (:http-method options :get)))))

(defn list-objects
  "List the objects in an S3 bucket. A optional map of options may be supplied.
  Available options are:
    :delimiter - read only keys up to the next delimiter (such as a '/')
    :marker    - read objects after this key
    :max-keys  - read only this many objects
    :prefix    - read only objects with this prefix

  The object listing will be returned as a map containing the following keys:
    :bucket          - the name of the bucket
    :prefix          - the supplied prefix (or nil if none supplied)
    :objects         - a list of objects
    :common-prefixes - the common prefixes of keys omitted by the delimiter
    :max-keys        - the maximum number of objects to be returned
    :truncated?      - true if the list of objects was truncated
    :marker          - the marker of the listing
    :next-marker     - the next marker of the listing"
  [cred bucket & [options]]
  (to-map
   (.listObjects
    (s3-client cred)
    (map->ListObjectsRequest (merge {:bucket bucket} options)))))

(defn delete-object
  "Delete an object from an S3 bucket."
  [cred bucket key]
  (.deleteObject (s3-client cred) bucket key))

(defn object-exists?
  "Returns true if an object exists in the supplied bucket and key."
  [cred bucket key]
  (try
    (get-object-metadata cred bucket key)
    true
    (catch AmazonServiceException e
      (if (= 404 (.getStatusCode e))
        false
        (throw e)))))

(defn copy-object
  "Copy an existing S3 object to another key. Returns a map containing
   the data returned from S3"
  ([cred bucket src-key dest-key]
     (copy-object cred bucket src-key bucket dest-key))
  ([cred src-bucket src-key dest-bucket dest-key]
     (to-map (.copyObject (s3-client cred) src-bucket src-key dest-bucket dest-key))))

(defn- map->ListVersionsRequest
  "Create a ListVersionsRequest instance from a map of values."
  [request]
  (doto (ListVersionsRequest.)
    (set-attr .setBucketName      (:bucket request))
    (set-attr .setDelimiter       (:delimiter request))
    (set-attr .setKeyMarker       (:key-marker request))
    (set-attr .setMaxResults      (maybe-int (:max-results request)))
    (set-attr .setPrefix          (:prefix request))
    (set-attr .setVersionIdMarker (:version-id-marker request))))

(defn list-versions
 "List the versions in an S3 bucket. A optional map of options may be supplied.
  Available options are:
    :delimiter         - the delimiter used in prefix (such as a '/')
    :key-marker        - read versions from the sorted list of all versions starting
                         at this marker.
    :max-results       - read only this many versions
    :prefix            - read only versions with keys having this prefix
    :version-id-marker - read objects after this version id

  The version listing will be returned as a map containing the following versions:
    :bucket                 - the name of the bucket
    :prefix                 - the supplied prefix (or nil if none supplied)
    :versions               - a sorted list of versions, newest first, each
                              version has:
                              :version-id     - the unique version id
                              :latest?        - is this the latest version for that key?
                              :delete-marker? - is this a delete-marker?
    :common-prefixes        - the common prefixes of keys omitted by the delimiter
    :max-results            - the maximum number of results to be returned
    :truncated?             - true if the results were truncated
    :key-marker             - the key marker of the listing
    :next-version-id-marker - the version ID marker to use in the next listVersions
                              request in order to obtain the next page of results.
    :version-id-marker      - the version id marker of the listing"
 [cred bucket & [options]]
 (to-map
   (.listVersions
    (s3-client cred)
    (map->ListVersionsRequest (merge {:bucket bucket} options)))))

(defn delete-version
  "Deletes a specific version of the specified object in the specified bucket."
  [cred bucket key version-id]
  (.deleteVersion (s3-client cred) bucket key version-id))

(defprotocol ^{:no-doc true} ToClojure
  "Convert an object into an idiomatic Clojure value."
  (^{:no-doc true} to-clojure [x] "Turn the object into a Clojure value."))

(extend-protocol ToClojure
  CanonicalGrantee
  (to-clojure [grantee]
    {:id           (.getIdentifier grantee)
     :display-name (.getDisplayName grantee)})
  EmailAddressGrantee
  (to-clojure [grantee]
    {:email (.getIdentifier grantee)})
  GroupGrantee
  (to-clojure [grantee]
    (condp = grantee
      GroupGrantee/AllUsers           :all-users
      GroupGrantee/AuthenticatedUsers :authenticated-users
      GroupGrantee/LogDelivery        :log-delivery))
  Permission
  (to-clojure [permission]
    (condp = permission
      Permission/FullControl :full-control
      Permission/Read        :read
      Permission/ReadAcp     :read-acp
      Permission/Write       :write
      Permission/WriteAcp    :write-acp)))

(extend-protocol Mappable
  Grant
  (to-map [grant]
    {:grantee    (to-clojure (.getGrantee grant))
     :permission (to-clojure (.getPermission grant))})
  AccessControlList
  (to-map [acl]
    {:grants (set (map to-map (.getGrants acl)))
     :owner  (to-map (.getOwner acl))}))

(defn get-bucket-acl
  "Get the access control list (ACL) for the supplied bucket. The ACL is a map
  containing two keys:
    :owner  - the owner of the ACL
    :grants - a set of access permissions granted

  The grants themselves are maps with keys:
    :grantee    - the individual or group being granted access
    :permission - the type of permission (:read, :write, :read-acp, :write-acp or
                  :full-control)."
  [cred ^String bucket]
  (to-map (.getBucketAcl (s3-client cred) bucket)))

(defn get-object-acl
  "Get the access control list (ACL) for the supplied object. See get-bucket-acl
  for a detailed description of the return value."
  [cred bucket key]
  (to-map (.getObjectAcl (s3-client cred) bucket key)))

(defn- permission [perm]
  (case perm
    :full-control Permission/FullControl
    :read         Permission/Read
    :read-acp     Permission/ReadAcp
    :write        Permission/Write
    :write-acp    Permission/WriteAcp))

(defn- grantee [grantee]
  (cond
   (keyword? grantee)
     (case grantee
      :all-users           GroupGrantee/AllUsers
      :authenticated-users GroupGrantee/AuthenticatedUsers
      :log-delivery        GroupGrantee/LogDelivery)
   (:id grantee)
     (CanonicalGrantee. (:id grantee))
   (:email grantee)
     (EmailAddressGrantee. (:email grantee))))

(defn- clear-acl [^AccessControlList acl]
  (doseq [grantee (->> (.getGrants acl)
                       (map #(.getGrantee ^Grant %))
                       (set))]
    (.revokeAllPermissions acl grantee)))

(defn- add-acl-grants [^AccessControlList acl grants]
  (doseq [g grants]
    (.grantPermission acl
      (grantee (:grantee g))
      (permission (:permission g)))))

(defn- update-acl [^AccessControlList acl funcs]
  (let [grants (:grants (to-map acl))
        update (apply comp (reverse funcs))]
    (clear-acl acl)
    (add-acl-grants acl (update grants))))

(defn- create-acl [permissions]
  (doto (AccessControlList.)
    (update-acl permissions)))

(defn update-bucket-acl
  "Update the access control list (ACL) for the named bucket using functions
  that update a set of grants (see get-bucket-acl).

  This function is often used with the grant and revoke functions, e.g.

    (update-bucket-acl cred bucket
      (grant :all-users :read)
      (grant {:email \"foo@example.com\"} :full-control)
      (revoke {:email \"bar@example.com\"} :write))"
  [cred ^String bucket & funcs]
  (let [acl (.getBucketAcl (s3-client cred) bucket)]
    (update-acl acl funcs)
    (.setBucketAcl (s3-client cred) bucket acl)))

(defn update-object-acl
  "Updates the access control list (ACL) for the supplied object using functions
  that update a set of grants (see update-bucket-acl for more details)."
  [cred ^String bucket ^String key & funcs]
  (let [acl (.getObjectAcl (s3-client cred) bucket key)]
    (update-acl acl funcs)
    (.setObjectAcl (s3-client cred) bucket key acl)))

(defn grant
  "Returns a function that adds a new grant map to a set of grants.
  See update-bucket-acl."
  [grantee permission]
  #(conj % {:grantee grantee :permission permission}))

(defn revoke
  "Returns a function that removes a grant map from a set of grants.
  See update-bucket-acl."
  [grantee permission]
  #(disj % {:grantee grantee :permission permission}))
