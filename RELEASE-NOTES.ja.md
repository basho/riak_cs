# Riak CS 1.5.0 リリースノート

## 新規追加

* A new command `riak-cs-debug` including `cluster-info` [riak_cs/#769](https://github.com/basho/riak_cs/pull/769), [riak_cs/#832](https://github.com/basho/riak_cs/pull/832)
* `cluster-info` 取得を含む新規コマンド `riak-cs-debug` を追加 [riak_cs/#769](https://github.com/basho/riak_cs/pull/769), [riak_cs/#832](https://github.com/basho/riak_cs/pull/832)
* Tie up all existing commands into a new command `riak-cs-admin` [riak_cs/#839](https://github.com/basho/riak_cs/pull/839)
* 既存コマンド群を新規コマンド `riak-cs-admin` へ統合 [riak_cs/#839](https://github.com/basho/riak_cs/pull/839)
* Add a command `riak-cs-admin stanchion` to switch Stanchion IP and port manually [riak_cs/#657](https://github.com/basho/riak_cs/pull/657)
* Stanchion の IP、ポートを変更する新規コマンド `riak-cs-admin stanchion` を追加 [riak_cs/#657](https://github.com/basho/riak_cs/pull/657)
* Performance of garbage collection has been improved via Concurrent GC [riak_cs/#830](https://github.com/basho/riak_cs/pull/830)
* 並行 GC によるガベージコレクション性能の向上 [riak_cs/#830](https://github.com/basho/riak_cs/pull/830)
* Iterator refresh [riak_cs/#805](https://github.com/basho/riak_cs/pull/805)
* Iterator refresh [riak_cs/#805](https://github.com/basho/riak_cs/pull/805)
* `fold_objects_for_list_keys` made default in Riak CS [riak_cs/#737](https://github.com/basho/riak_cs/pull/737), [riak_cs/#785](https://github.com/basho/riak_cs/pull/785)
* `fold_objects_for_list_keys` 設定をデフォルト有効に変更 [riak_cs/#737](https://github.com/basho/riak_cs/pull/737), [riak_cs/#785](https://github.com/basho/riak_cs/pull/785)
* Add support for Cache-Control header [riak_cs/#821](https://github.com/basho/riak_cs/pull/821)
* Cache-Control ヘッダーのサポートを追加 [riak_cs/#821](https://github.com/basho/riak_cs/pull/821)
* Allow objects to be reaped sooner than leeway interval. [riak_cs/#470](https://github.com/basho/riak_cs/pull/470)
* 猶予期間(`leeway_seconds`)内でもオブジェクトをガベージコレクション可能にする変更 [riak_cs/#470](https://github.com/basho/riak_cs/pull/470)
* PUT Copy on both objects and upload parts [riak_cs/#548](https://github.com/basho/riak_cs/pull/548)
* オブジェクト、マルチパートともに PUT Copy API を追加 [riak_cs/#548](https://github.com/basho/riak_cs/pull/548)
* Update to lager 2.0.3
* lager 2.0.3 へ更新
* Compiles with R16B0x (Releases still by R15B01)
* R16B0x をビルド環境に追加 (リリースは R15B01 でビルド)
* `gc_paginated_index` 設定をデフォルト有効に変更 [riak_cs/#881](https://github.com/basho/riak_cs/issues/881)
* Add new API: Delete Multiple Objects [riak_cs/#728](https://github.com/basho/riak_cs/pull/728)
* 新規 API: Delete Multiple Objects の追加[riak_cs/#728](https://github.com/basho/riak_cs/pull/728)
* Add warning logs for manifests, siblings, bytes and history [riak_cs/#915](https://github.com/basho/riak_cs/pull/915)
* マニフェストに対して siblings, バイト、履歴の肥大化を警告するログ追加 [riak_cs/#915](https://github.com/basho/riak_cs/pull/915)

## 修正されたバグ

* Align `ERL_MAX_PORTS` with Riak default: 64000 [riak_cs/#636](https://github.com/basho/riak_cs/pull/636)
* `ERL_MAX_PORTS` を Riak のデフォルトに合わせ 64000 へ変更 [riak_cs/#636](https://github.com/basho/riak_cs/pull/636)
* Allow Riak CS admin resources to be used with OpenStack API [riak_cs/#666](https://github.com/basho/riak_cs/pull/666)
* Riak CS 管理リソースを OpenStack API でも利用可能にする修正 [riak_cs/#666](https://github.com/basho/riak_cs/pull/666)
* Fix path substitution code to fix Solaris source builds [riak_cs/#733](https://github.com/basho/riak_cs/pull/733)
* Solaris でのソースビルドのバグ修正のため、パス代入コードの変更 [riak_cs/#733](https://github.com/basho/riak_cs/pull/733)
* `sanity_check(true,false)` logs invalid error on `riakc_pb_socket` error [riak_cs/#683](https://github.com/basho/riak_cs/pull/683)
* `riakc_pb_socket` エラー時の `sanity_check(true,false)` バグを修正 [riak_cs/#683](https://github.com/basho/riak_cs/pull/683)
* Riak-CS-GC timestamp for scheduler is in the year 0043, not 2013. [riak_cs/#713](https://github.com/basho/riak_cs/pull/713) fixed by [riak_cs/#676](https://github.com/basho/riak_cs/pull/676)
* Riak-CS-GC のスケジューラタイムスタンプが 2013 ではなく 0043 になるバグを修正. [riak_cs/#713](https://github.com/basho/riak_cs/pull/713) fixed by [riak_cs/#676](https://github.com/basho/riak_cs/pull/676)
* Excessive calls to OTP code_server process #669 fixed by [riak_cs/#675](https://github.com/basho/riak_cs/pull/675)
* OTP code_server プロセスへの過大な呼び出しを修正 [riak_cs/#675](https://github.com/basho/riak_cs/pull/675)
* Return HTTP 400 if content-md5 does not match [riak_cs/#596](https://github.com/basho/riak_cs/pull/596)
* content-md5 が一致しない場合に HTTP 400 を返すよう修正 [riak_cs/#596](https://github.com/basho/riak_cs/pull/596)
* `/riak-cs/stats` and `admin_auth_enabled=false` don't work together correctly. [riak_cs/#719](https://github.com/basho/riak_cs/pull/719)
* `/riak-cs/stats` が `admin_auth_enabled=false` の時に動作しなバグを修正. [riak_cs/#719](https://github.com/basho/riak_cs/pull/719)
* Storage calculation doesn't handle tombstones, nor handle undefined manifest.props [riak_cs/#849](https://github.com/basho/riak_cs/pull/849)
* ストレージ計算で tombstone および undefined の manifest.props を処理できないバグを修正 [riak_cs/#849](https://github.com/basho/riak_cs/pull/849)
* MP initiated objects remains after delete/create buckets #475 fixed by [riak_cs/#857](https://github.com/basho/riak_cs/pull/857) and [stanchion/#78](https://github.com/basho/stanchion/pull/78)
* 開始されたマルチパートオブジェクトが、バケットの削除、作成後にも残るバグを修正 [riak_cs/#857](https://github.com/basho/riak_cs/pull/857) and [stanchion/#78](https://github.com/basho/stanchion/pull/78)
* handling empty query string on list multipart upload [riak_cs/#843](https://github.com/basho/riak_cs/pull/843)
* list multipart upload の空クエリパラメータの扱いを修正 [riak_cs/#843](https://github.com/basho/riak_cs/pull/843)
* Setting ACLs via headers at PUT Object creation [riak_cs/#631](https://github.com/basho/riak_cs/pull/631)
* PUT Object 時にヘッダ指定の ACL が設定されないバグを修正 [riak_cs/#631](https://github.com/basho/riak_cs/pull/631)
* Improve handling of poolboy timeouts during ping requests [riak_cs/#763](https://github.com/basho/riak_cs/pull/763)
* ping リクエストの poolboy タイムアウト処理を改善 [riak_cs/#763](https://github.com/basho/riak_cs/pull/763)
* Remove unnecessary log message on anonymous access [riak_cs/#876](https://github.com/basho/riak_cs/issues/876)
* 匿名アクセス時の不要なログを削除 [riak_cs/#876](https://github.com/basho/riak_cs/issues/876)
* Fix inconsistent ETag on objects uploaded by multipart [riak_cs/#855](https://github.com/basho/riak_cs/issues/855)
* マルチパートでアップロードされたオブジェクトの ETag 不正を修正 [riak_cs/#855](https://github.com/basho/riak_cs/issues/855)
* Fix policy version validation in PUT Bucket Policy [riak_cs/#911](https://github.com/basho/riak_cs/issues/911)
* PUT Bucket Policy のポリシーバージョン確認の不具合を修正[riak_cs/#911](https://github.com/basho/riak_cs/issues/911)
* Fix return code of several commands, to return 0 for success [riak_cs/#908](https://github.com/basho/riak_cs/issues/908)
* コマンド成功時に終了コード 0 を返すよう修正 [riak_cs/#908](https://github.com/basho/riak_cs/issues/908)


## Notes on Upgrading

### Incomplete multipart uploads

[riak_cs/#475](https://github.com/basho/riak_cs/issues/475) was a
security issue where a newly created bucket may include unaborted or
incomplete multipart uploads which was created in previous epoch of
the bucket with same name. This was fixed by:

- on creating buckets; checking if live multipart exists and if
  exists, return 500 failure to client.

- on deleting buckets; trying to clean up all live multipart remains,
  and checking if live multipart remains (in stanchion). if exists,
  return 409 failure to client.

Note that a few operations are needed after upgrading from 1.4.x (or
former) to 1.5.0.

- run `riak_cs_console:cleanup_orphan_multipart/0` or
  `riak_cs_console:cleanup_orphan_multipart/1` in an attached console
  to cleanup all buckets
- there might be a time period until above cleanup finished, where no
  client can create bucket if unfinished multipart upload remains
  under deleted bucket. You can find [critical] log if such bucket
  creation is attempted.

### Leeway seconds and disk space

[riak_cs/#470](https://github.com/basho/riak_cs/pull/470) changed the
behaviour of object deletion and garbage collection. The timestamps in
garbage collection bucket were changed from the current time when the
object is deleted, to the future time when the object is to be
deleted. Garbage collector was also changed to collect objects until
'now - leeway seconds', from collecting objects until 'now'.

Before:

```
           t1                         t2
-----------+--------------------------+------------------->
           DELETE object:             GC triggered:
           marked as                  collects objects
           "t1+leeway"                marked as "t2"
```

After:

```
           t1                         t2
-----------+--------------------------+------------------->
           DELETE object:             GC triggered:
           marked as "t1"             collects objects
           in GC bucket               marked as "t2 - leeway"
```

This leads that there exists a period where no objects are collected
right after upgrade to 1.5.0, say, `t0`, until `t0 + leeway` . And
objects deleted just before `t0` won't be collected until `t0 +
2*leeway` .

Also, all CS nodes which run GC should be upgraded *first.* CS nodes
which do not run GC should be upgraded later, to let leeway second
system work properly. Or stop GC while upgrading whole cluster, by
running `riak-cs-gc interval infinity` .

Multi data center cluster should be upgraded more carefully, as to
make sure GC is not running while upgrading.

# Riak CS 1.4.5 リリースノート

## 修正されたバグ

* list objects v2 fsm のいくつかのデータが「見えない」バグを修正 [riak_cs/788](https://github.com/basho/riak_cs/pull/788)
* HEADリクエスト時にアクセス集計していた問題を修正 [riak_cs/791](https://github.com/basho/riak_cs/pull/791)
* POST/PUTリクエスト時のXML中の空白文字の対処 [riak_cs/795](https://github.com/basho/riak_cs/pull/795)
* ストレージ使用量計算時の誤ったバケット名を修正 [riak_cs/800](https://github.com/basho/riak_cs/pull/800)
  Riak CS 1.4.4 で混入したバグにより、そのバージョンを使用している期間の
  ストレージ計算はバケット名が文字列 "struct" に置き換わった結果となっていました。
  本バージョン 1.4.5 でこのバグ自体は修正されましたが、すでに計算済みの古い結果を
  さかのぼって修正することは不可能です。バケット名が "struct" に置き換わってしまった
  計算結果では、個別バケットの使用量を知ることはできませんが、その場合であっても
  個々のユーザに関して所有バケットにわたる合計は正しい数字を示します。
* Unicodeのユーザ名とXMLの対応 [riak_cs/807](https://github.com/basho/riak_cs/pull/807)
* ストレージ使用量で必要なXMLフィールドを追加 [riak_cs/808](https://github.com/basho/riak_cs/pull/808)
* オブジェクトのfoldのタイムアウトを揃えた [riak_cs/811](https://github.com/basho/riak_cs/pull/811)
* 削除されたバケットをユーザーのレコードから削除 [riak_cs/812](https://github.com/basho/riak_cs/pull/812)

## 新規追加

* オブジェクト一覧表示のv2 FSMでプレフィクスを使用する最適化を追加 [riak_cs/804](https://github.com/basho/riak_cs/pull/804)

# Riak CS 1.4.4 リリースノート

これはバグフィックスのためのリリースです。統計計算の修正が含まれています。

## 修正されたバグ

* basho-patches ディレクトリが作成されなかった問題を修正 [riak_cs/775](https://github.com/basho/riak_cs/issues/775) .

* `sum_bucket` のタイムアウトが全ての容量計算をクラッシュさせていた問題を修正 [riak_cs/759](https://github.com/basho/riak_cs/issues/759) .

* アクセス集計のスロットリング失敗を修正 [riak_cs/758](https://github.com/basho/riak_cs/issues/758) .

* アクセス集計のクラッシュを修正 [riak_cs/747](https://github.com/basho/riak_cs/issues/747) .


# Riak CS 1.4.3 リリースノート

## 修正された問題

- schedule_delete状態のマニフェストがpending_deleteやactive状態へ復帰するバグを修正。
- 上書きによって既に削除されたマニフェストをカウントしない。
- 誤ったmd5による上書き操作で、既存バージョンのオブジェクトを削除しない。

## 新規追加

- マニフェストプルーニングのパフォーマンス改善。
- GCにおける2iのページングオプションを追加。GC対象データ収集時のタイムアウト対策。
- ブロック取得処理における接続断のハンドリングを改善。
- lager 2.0.1へのアップデート。
- 時刻によるマニフェストプルーニングに個数オプションを追加。
- 複数アクセスアーカイブプロセスの並行実行を許可。

# Riak CS 1.4.2 リリースノート

## 修正された問題

- Debian Linux 上の Enterprise 版ビルドの問題を修正。
- ソース tarball ビルドの問題を修正。
- アクセス統計において、正常アクセスがエラーと扱われてしまうバグを修正。
- Riak バージョン 1.4 以前とあわせて動作するよう、バケットリスト
  map フェーズのログを lager バージョンに依存しないよう変更。
- Riak CS 1.3.0 以前で保存されたマニフェストについて、 `props` フィールド
  の `undefined` を正しく扱うよう修正。

## 新規追加

- 最初のガベージコレクションの遅延を設定する `initial_gc_delay` オプションを追加。
- ガベージコレクションバケットのキーにランダムなサフィックスを追加し、
  ホットキーの回避と削除の性能を向上。
- マニフェストに cluster id が指定されていない場合に用いる
  `default_proxy_cluster_id` オプションを追加。OSS 版から Enterprise 版への
  移行が容易になる。

# Riak CS 1.4.1 リリースノート

## 修正された問題

- 最初の1002個のキーがpending delete状態だったときにlist objectsがクラッシュ
  する問題を修正
- GCデーモンがクラッシュする問題を解決
- node_packageをアップデートしパッケージ作成の問題を解決

# Riak CS 1.4.0 リリースノート

## 修正された問題

- GCバケットで使われていないキーを削除
- マルチパートアップロードのクエリ文字での認証を修正
- マルチパートでアップロードされたオブジェクトのストレージクラスを修正
- マルチパートアップロードされたオブジェクトのetagsを修正
- Riak CSのマルチバックエンドのインデックス修正をサポート
- GETリクエストの際、通信が遅い場合のメモリ増大を修正
- アクセス統計処理のメモリ使用量を削減
- オブジェクトのACL HEADリクエストの際の500を修正
- マルチパートでアップロードされたオブジェクトの並列アップロードや削除の際の競合の問題を解決
- Content-md5のヘッダがあった場合に整合性をチェックするように修正
- Riakとのコネクションが切れた際のハンドリングを修正

## 新規追加

- Swift APIとKeystone認証のサポートを試験的に追加
- Riak 1.4.0以降と併用された場合のオブジェクト一覧取得のパフォーマンスを改善
- ユーザーアカウント名とメールアドレスは変更可能に
- データセンタ間レプリケーションv3のサポートを追加
- Riakとのコネクションタイムアウトを変更可能に
- Lagerのsyslogサポートを追加
- データブロックへのリクエスト時は1つのvnodeへアクセス

# Riak CS 1.3.1 Release Notes

## Bugs Fixed

- Fix bug in handling of active object manifests in the case of
  overwrite or delete that could lead to old object versions being
  resurrected.
- Fix improper capitalization of user metadata header names.
- Fix issue where the S3 rewrite module omits any query parameters
  that are not S3 subresources. Also correct handling of query
  parameters so that parameter values are not URL decoded twice. This
  primarily affects pre-signed URLs because the access key and request
  signature are included as query parameters.
- Fix for issue with init script stop.

# Riak CS 1.3.0 Release Notes

## Bugs Fixed

- Fix handling of cases where buckets have siblings. Previously this
  resulted in 500 errors returned to the client.
- Reduce likelihood of sibling creation when creating a bucket.
- Return a 404 instead of a 403 when accessing a deleted object.
- Unquote URLs to accommodate clients that URL encode `/` characters
  in URLs.
- Deny anonymous service-level requests to avoid unnecessary error
  messages trying to list the buckets owned by an undefined user.

## Additions

- Support for multipart file uploads. Parts must be in the range of
  5MB-5GB.
- Support for bucket policies using a restricted set of principals and
  conditions.
- Support for returning bytes ranges of a file using the Range header.
- Administrative commands may be segrated onto a separate interface.
- Authentication for administrative commands may be disabled.
- Performance and stability improvements for listing the contents of
  buckets.
- Support for the prefix, delimiter, and marker options when listing
  the contents of a bucket.
- Support for using Webmachine's access logging features in
  conjunction with the Riak CS internal access logging mechanism.
- Moved all administrative resources under /riak-cs.
- Riak CS now supports packaging for FreeBSD, SmartOS, and Solaris.

# Riak CS 1.2.2 Release Notes

## Bugs Fixed

- Fix problem where objects with utf-8 unicode key cannot be listed
  nor fetched.
- Speed up bucket_empty check and fix process leak. This bug was
  originally found when a user was having trouble with `s3cmd
  rb s3://foo --recursive`. The operation first tries to delete the
  (potentially large) bucket, which triggers our bucket empty
  check. If the bucket has more than 32k items, we run out of
  processes unless +P is set higher (because of the leak).

## Additions

- Full support for MDC replication

# Riak CS 1.2.1 Release Notes

## Bugs Fixed

- Return 403 instead of 404 when a user attempts to list contents of
  nonexistent bucket.
- Do not do bucket list for HEAD or ?versioning or ?location request.

## Additions

- Add reduce phase for listing bucket contents to provide backpressure
  when executing the MapReduce job.
- Use prereduce during storage calculations.
- Return 403 instead of 404 when a user attempts to list contents of
  nonexistent bucket.

# Riak CS 1.2.0 Release Notes

## Bugs Fixed

- Do not expose stack traces to users on 500 errors
- Fix issue with sibling creation on user record updates
- Fix crash in terminate state when fsm state is not fully populated
- Script fixes and updates in response to node_package updates

## Additions

- Add preliminary support for MDC replication
- Quickcheck test to exercise the erlcloud library against Riak CS
- Basic support for riak_test integration

# Riak CS 1.1.0 Release Notes

## Bugs Fixed

- Check for timeout when checking out a connection from poolboy.
- PUT object now returns 200 instead of 204.
- Fixes for Dialyzer errors and warnings.
- Return readable error message with 500 errors instead of large webmachine backtraces.

## Additions

- Update user creation to accept a JSON or XML document for user
  creation instead of URL encoded text string.
- Configuration option to allow anonymous users to create accounts. In
  the default mode, only the administrator is allowed to create
  accounts.
- Ping resource for health checks.
- Support for user-specified metadata headers.
- User accounts may be disabled by the administrator.
- A new key_secret can be issued for a user by the administrator.
- Administrator can now list all system users and optionally filter by
  enabled or disabled account status.
- Garbage collection for deleted and overwritten objects.
- Separate connection pool for object listings with a default of 5
  connections.
- Improved performance for listing all objects in a bucket.
- Statistics collection and querying.
- DTrace probing.

# Riak CS 1.0.2 Release Notes

## Additions

- Support query parameter authentication as specified in [[http://docs.amazonwebservices.com/AmazonS3/latest/dev/RESTAuthentication.html][Signing and Authenticating REST Requests]].

# Riak CS 1.0.1 Release Notes

## Bugs Fixed

- Default content-type is not passed into function to handle PUT
  request body
- Requests hang when a node in the Riak cluster is unavailable
- Correct inappropriate use of riak_moss_utils:get_user by
  riak_moss_acl_utils:get_owner_data

# Riak CS 1.0.0 Release Notes

## Bugs Fixed

- Fix PUTs for zero-byte files
- Fix fsm initialization race conditions
- Canonicalize the entire path if there is no host header, but there are
  tokens
- Fix process and socket leaks in get fsm

## Other Additions

- Subsystem for calculating user access and storage usage
- Fixed-size connection pool of Riak connections
- Use a single Riak connection per request to avoid deadlock conditions
- Object ACLs
- Management for multiple versions of a file manifest
- Configurable block size and max content length
- Support specifying non-default ACL at bucket creation time

# Riak CS 0.1.2 Release Notes

## Bugs Fixed

- Return 403 instead of 503 for invalid anonymous or signed requests.
- Properly clean up processes and connections on object requests.

# Riak CS 0.1.1 Release Notes

## Bugs Fixed

- HEAD requests always result in a `403 Forbidden`.
- `s3cmd info` on a bucket object results in an error due to missing
  ACL document.
- Incorrect atom specified in `riak_moss_wm_utils:parse_auth_header`.
- Bad match condition used in `riak_moss_acl:has_permission/2`.

# Riak CS 0.1.0 Release Notes

## Bugs Fixed

- `s3cmd info` fails due to missing `'last-modified` key in return document.
- `s3cmd get` of 0 byte file fails.
- Bucket creation fails with status code `415` using the AWS Java SDK.

## Other Additions

- Bucket-level access control lists
- User records have been modified so that an system-wide unique email
  address is required to create a user.
- User creation requests are serialized through `stanchion` to be
  certain the email address is unique.
- Bucket creation and deletion requests are serialized through
  `stanchion` to ensure bucket names are unique in the system.
- The `stanchion` serialization service is now required to be installed
  and running for the system to be fully operational.
- The concept of an administrative user has been added to the system. The credentials of the
  administrative user must be added to the app.config files for `moss` and `stanchion`.
- User credentials are now created using a url-safe base64 encoding module.

## Known Issues

- Object-level access control lists have not yet been implemented.

# Riak CS 0.0.3 Release Notes

## Bugs Fixed

- URL decode keys on put so they are represented correctly. This
  eliminates confusion when objects with spaces in their names are
  listed and when attempting to access them.
- Properly handle zero-byte files
- Reap all processes during file puts

## Other Additions

- Support for the s3cmd subcommands sync, du, and rb

 - Return valid size and checksum for each object when listing bucket objects.
 - Changes so that a bucket may be deleted if it is empty.

- Changes so a subdirectory path can be specified when storing or retrieving files.
- Make buckets private by default
- Support the prefix query parameter
- Enhance process dependencies for improved failure handling

## Known Issues

- Buckets are marked as /private/ by default, but globally-unique
  bucket names are not enforced. This means that two users may
  create the same bucket and this could result in unauthorized
  access and unintentional overwriting of files. This will be
  addressed in a future release by ensuring that bucket names are
  unique across the system.
