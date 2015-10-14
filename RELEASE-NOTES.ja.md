# Riak S2 (Riak CS) 2.1.0 リリースノート

2015/10/13 リリース

これは後方互換のリリースです。新しいメトリクスシステム、ガーベジコレクションの改良、その他の新しい機能が追加されています。Riak S2 2.1 はRiak KV 2.0.5 以上、および 2.1.1 以上で動作するようデザインされています。

**注意:** このリリースは Riak S2 2.x シリーズに対してのみ、後方互換性があります。

### Riak KV 2.1.1 の利用

Riak KV 2.1.1 は `riak_cs_kv_multi_backend` のコピーを含むため、 `multi_backend` と `add_paths` 設定を advanced.config へ追加設定する必要はなくなりました。

代わりに下記をriak.confへ設定します:

```
storage_backend = prefix_multi
cs_version = 20100
```

なお、ストレージ集計機能を利用する場合は、MapReduceのコードを読み込むために `add_paths` 設定は依然として必要です。


## 新機能
### メトリクス

新しいメトリクスが追加されました。これにより Riak S2 システムの健全性把握や、ユーザーもしくはバケット毎のストレージ使用量レポートが利用可能です。利用できるメトリクスは次の通り:
  * S3 APIのレイテンシとカウンター
  * Stanchionのレイテンシとカウンター
  * Riak Erlangクライアントのレイテンシとカウンター(Riak S2 - Riak間通信)
  * コネクションプールの情報(active数, idle数, overflow数)
  * システム情報: ライブラリのversion、port数、プロセス数
  * Erlang VMのメモリ情報
  * HTTPリスナ情報: アクティブソケットと待ち状態のアクセプタ

**注意:** メトリクスのアイテム名は 2.0.x 以前のままではなく、変更もしくは削除されています。この点では、以前のバージョンへの一貫性がありません。詳細は [ドキュメント(英語)](docs.basho.com/riakcs/latest/cookbooks/Monitoring-and-Metrics/) をご覧ください。

* [[PR 1189](https://github.com/basho/riak_cs/pull/1189)]
* [[PR 1180](https://github.com/basho/riak_cs/pull/1180)]
* [[PR 1214](https://github.com/basho/riak_cs/pull/1214)]
* [[PR 1194](https://github.com/basho/riak_cs/pull/1194)]
* [[PR 99](https://github.com/basho/stanchion/pull/99)]

これまでのメトリクスに加え、新しくストレージ使用量メトリクスも利用できるようになりました。このメトリクスはストレージ集計中に収集されます。デフォルトでメトリクス収集は無効化されていますが、advanced.config で `detailed_storage_calc` を `true` へ変更すると有効化できます。 このオプションを有効化すると、APIからは見えないマニフェストデータの情報にアクセスできます。これは`writing`、`pending_delete`、`scheduled_delete`、`active`の各状態にいくつのマニフェストが属しているか、という情報です。

**注意:** メトリクスは常に実際のディスク使用量を正しく反映している訳ではありません。例えば、`writing`は実際に使われている容量より大きい値を指すかもしれません。もしくはアップロードが途中で中止されると、集計処理は実際に消費されているストレージの容量がわかりません。同様に、ブロックは既にガベージコレクションによって部分的に削除されているかもしれないたため、`scheduled_delete`もディスク使用量を正しく反映していません。

* [[PR 1120](https://github.com/basho/riak_cs/pull/1120)]

###`riak-cs-admin`
次の管理CLIは [`riak-cs-admin` command](http://docs.basho.com/riakcs/latest/cookbooks/command-line-tools/) によって置き換えられました:

* `riak-cs-storage`
* `riak-cs-gc`
* `riak-cs-access`
* `riak-cs-stanchion`

これらのコマンドは廃止予定のため、今後のリリースで削除されます。

* [[PR 1175](https://github.com/basho/riak_cs/pull/1175)]

## ガベージコレクションの改良
いくつかの新しいオプションが `riak-cs-admin gc` コマンドへ追加されました:

* `active_delete_threshold` はマニフェストとブロックの削除をガベージコレクタへ委譲しないオプションです。これにより、小さいオブジェクトの削除という責務から、ガベージコレクタは解放されます。パフォーマンス改善の効果が得られるのは、ガベージコレクタが Delete Object API の呼び出しに追いつかない場合と、ガベージコレクタの実行時間が、小さいオブジェクトの削除に強く影響される場合の双方です。[[PR 1174](https://github.com/basho/riak_cs/pull/1174)]
* `--start` と `--end` オプションが `riak-cs-admin gc batch` コマンドへ追加されました。これはGCのマニュアルバッチ実行において処理対象を開始時刻、終了時刻で指定します。`--start` フラグは advanced.config の `epoch_start` 設定を上書く点に注意して下さい。[[PR 1147 ](https://github.com/basho/riak_cs/pull/1147)]
* `--leeway` は猶予期間(leeway period)の値を一時的に設定します。これは一度だけ利用され、繰返して次の実行で利用されることはありません。そして、`--max-workers` はガベージコレクタ実行の並列度を一時的に変更します。[[PR 1147 ](https://github.com/basho/riak_cs/pull/1147)]
* Riak S2 2.0 (とそれ以前は) Fullsyncレプリケーションとガベージコレクションの競合状態の問題があり、削除したブロックが一旦復活し、二度と削除できなくなる可能性がありました。Realtimeレプリケーションと、Realtimeレプリケーションのキューから溢れたGCバケット内エントリのレプリカが混在すると、sink側でブロックが収集されずに残るのです。他にも、GCとFullsyncレプリケーションが並行に動き、同時に同一のブロックやマニフェストを処理した場合にも、これが起こり得ます。Riak S2 2.1 ではFullsyncレプリケーションを避ける、決定論的ガベージコレクションを導入しました。そして、sink側、souce側の双方で同期したオブジェクトの収集、削除を実施するために、ガベージコレクションの `riak-cs-admin gc batch` で、 `--start` および `--end` フラグによる、期間指定ができるようになりました。 [[PR 1147 ](https://github.com/basho/riak_cs/pull/1147)]
* `riak-cs-admin gc earliest-keys` が利用できるようになりました。これによりガベージコレクションにて、 `epoch_start` 以降の最も古いエントリを見つけることができます。このオプションにより、ガベージコレクションの進捗を得ることができます。[[PR 1160](https://github.com/basho/riak_cs/pull/1160)]

ガベージコレクションの詳細は [ドキュメント（英語）](http://docs.basho.com/riakcs/latest/cookbooks/garbage-collection/) をご覧ください。

## 追加
### オープンソース
* MapReduceにおいて Riakオブジェクト取得の最適化が Riak 2.1 で導入されました。Riak S2 2.1 では今回、ストレージ集計で最適化を利用するオプションが導入されています。デフォルトでは無効化されていますが、advanced.config で `use_2i_for_storage_calc` を `true` にすると利用できます。これは LevelDB の I/O を 50% 減少させます。[[PR 1089](https://github.com/basho/riak_cs/pull/1089)]
* Erlang/OTP 17 のサポートが含まれます。[[PR 1245](https://github.com/basho/riak_cs/pull/1245) および [PR 1040](https://github.com/basho/stanchion/pull/1040)]
* ユーザーアクセス、および使用量を制限する、モジュールレベルのフックポイントが利用可能になりました。非常に試験的で、シンプルなノードレベルのサンプルモジュールも含まれています。オペレータは要求に応じたプラグインを作成したり、クオータ制限、アクセスレート制限、帯域制限といった、異なるモジュールを組み合わせることが可能です。 [[PR 1118](https://github.com/basho/riak_cs/pull/1118)]
*  orphanedブロックスキャナーの導入 [[PR 1133](https://github.com/basho/riak_cs/pull/1133)]
* `riak-cs-admin audit-bucket-ownership` はユーザーと、それに追加されたバケット間の整合性をチェックする新しいツールです。例えば、List Bucketで見えるBucketへアクセスができない場合や、Bucketが見えるが、これが削除できない場合に利用されます。[[PR 1202](https://github.com/basho/riak_cs/pull/1202)]
* 次のログローテーション設定がcuttlefishへ追加されました:
    * log.console.size
    * log.console.rotation
    * log.console.rotation.keep
    * log.error.rotation
    * log.error.rotation.keep
    * log.error.size

[[PR 1164](https://github.com/basho/riak_cs/pull/1164) および [PR 97](https://github.com/basho/stanchion/pull/97)]

* `riak_cs_wm_common` がデフォルトのコールバック `multiple_choices` を持ちます。これは `code_server` がボトルネックになるのを防止します。[[PR 1181](https://github.com/basho/riak_cs/pull/1181)]
* 認証前のユーザー取得の `PR=all` オプションは単に `PR=one` で置き換えられました。このオプションはレイテンシを改善します。特に、遅い(実際にはダウンしている)ノードがリクエスト全体の流れを`PR=all`によって妨げている場合に顕著です。これを有効化すると、ユーザーの所有するバケット一覧はバケットの削除後に切り詰められなくなり、代わりに削除が単にマークされます。[[PR 1191](https://github.com/basho/riak_cs/pull/1191)]
* infoログがストレージ集計バッチの開始時に追加されました。 [[PR 1238](https://github.com/basho/riak_cs/pull/1238)]
* `GET Bucket` リクエストがわかりやすいレスポンスになりました。Bucket lifecycle への 501のスタブとBucket requestPeymentへの単純なスタブが追加されています。[[PR 1223](https://github.com/basho/riak_cs/pull/1223)]
* ユーザフレンドリーないくつかの機能が [`riak-cs-debug`](http://docs.basho.com/riakcs/latest/cookbooks/command-line-tools/) へ追加されました。収集する情報の細やかなオプション、ユーザーの定義する設定ファイルへのフィルタ、そしてコマンドが失敗した際の詳細なアウトプットが利用できます。[[PR 1236](https://github.com/basho/riak_cs/pull/1236)]

### エンタープライズ
* MDC は `proxy_get` 機能を持っており、ブロックオブジェクトが要求されると、siteクラスタへ伝搬されます。今回、MDCのmultibag機能が `proxy_get` をサポートします。[PR 1171](https://github.com/basho/riak_cs/pull/1171) および [PR 25](https://github.com/basho/riak_cs_multibag/pull/25)]
* Multibag は "Supercluster" へ名称が変更されました。bag はMDCで複製されたRiakクラスターのセットを意味していましたが、これは新しく **superclusterのmember** へと呼び名が変わります。`riak-cs-multibag` コマンドも同じく `riak-cs-supercluster` へ名称変更されます。[[PR 1257](https://github.com/basho/riak_cs/pull/1257)], [[PR 1260](https://github.com/basho/riak_cs/pull/1260)], [[PR 106](https://github.com/basho/stanchion/pull/106)], [[PR 107](https://github.com/basho/stanchion/pull/107)] and [[PR 31](https://github.com/basho/riak_cs_multibag/pull/31)].
* いくつかの診断ツール、もしくは既知の問題へ対処する内部ツールがいくつか追加されました。
  [[PR 1145](https://github.com/basho/riak_cs/pull/1145),
  [PR 1134](https://github.com/basho/riak_cs/pull/1134),
  [PR 1133](https://github.com/basho/riak_cs/pull/1133)]
* マニフェストやブロックのSiblingを手動で解決する汎用の関数が追加されました。[[PR 1188](https://github.com/basho/riak_cs/pull/1188)]


## 変更
* Riak S2 と Stanchion の依存ライブラリが更新されました: cuttlefish 2.0.4, node_package 2.0.3, riak-erlang-client 2.1.1, lager 2.2.0, lager_syslog 2.1.1, eper 0.92 (Basho patched), cluster_info 2.0.3, riak_repl_pb_api 2.1.1, and riak_cs_multibag 2.1.0. [[PR 1190](https://github.com/basho/riak_cs/pull/1190), [PR 1197 ](https://github.com/basho/riak_cs/pull/1197), [PR 27](https://github.com/basho/riak_cs_multibag/pull/27), [PR 1245](https://github.com/basho/riak_cs/pull/1245), and [PR 104](https://github.com/basho/stanchion/pull/104)].
* Riak S2 はFolsomからExometerへ移行しました。[[PR 1165](https://github.com/basho/riak_cs/pull/1165) および [PR 1180](https://github.com/basho/riak_cs/pull/1180)]
* クライアントからのGetリクエストでブロック取得のエラー追跡が改善されました。クライアントからGetリクエストが発行された際、blockを解決するロジックは複雑です。はじめにRiak S2 はブロックを`n_val=1`で取得しようとします。これに失敗すると`n_val=3`で再取得を試みます。ブロックがローカルで取得できず、かつ`proxy_get`が有効で、データセンターレプリケーションが設定されているシステムでは、Riak S2 はリモートサイトへのproxy getを試みます。このフォールバック、およびリトライのロジックは複雑で追跡が難しく、特に故障や不安定な状況ではこれは顕著です。今回の改善はエラーの追跡を前述の処理全体へ追加しており、問題解決を助けます。とりわけ、各ブロックにおいて、ブロックサーバはRiakクライアントからの全エラーを積み上げ、エラー発生箇所の型呼び出しと、これら全ての詳細(reason)を通知します。[[PR 1177](https://github.com/basho/riak_cs/pull/1177)]
* バケット内オブジェクトの一覧取得において、prefixを指定した `GET Bucket` APIの利用は最適化が必要でした。Riak内のオブジェクトの走査に使用する、終了key指定が非常に大雑把だったのです。この変更により、Riakのオブジェクトを走査する際、終了keyがより厳しく指定され、不必要なvnode内での走査処理が回避されます。[[PR 1233](https://github.com/basho/riak_cs/pull/1233)]
* keyの最大長の制限が導入されました。この制限はデフォルトで1024バイトです。これはriak-cs.confの `max_key_length` で明示的に1024より大きい値を指定しない限り、PUT、GET、DELETEで1024バイトを超えるkeyが処理されない事を意味します。keyの長さに関して以前の振る舞いを維持したい場合は、`max_key_length` を `unlimited` に指定しなければなりません。 [[PR 1233](https://github.com/basho/riak_cs/pull/1233)]
* 複数ノードがダウンした不完全なクラスタにおいて、誤ったnotfoundにより、ブロックがすでに削除されたと誤認識する問題がありました。これはブロックのリークを引き起こします。この問題を回避するため、PRのデフォルトは `quorum` に設定されました。そして、PWのデフォルトは `1` になり、少なくとも一つのブロックのレプリカは、プライマリノードへ書かれるようなりました。 これに加え、クラスタ内に存在しないオブジェクトの、特定のブロックが "not found" エラーを返した時に、今回の対処はブロックサーバのクラッシュを適切に防ぎます。代わりに、到達不能なブロックはスキップされ、残されたブロックとマニフェストは収集されます。ブロックでPRとPWの値が増えたため、 PUTの可用性とGCのスループットは減少する可能性があります。到達不能な少数のRiakノードがPUTリクエストが成功を返すことを阻害したり、すべての到達不能なノードが戻るまでガベージコレクションによる全ブロックの収集を阻害するかもしれません。[[PR 1242](https://github.com/basho/riak_cs/pull/1242)]
* 同時かつ、無期限で呼び出される `gen_fsm` のいくつかの関数へ無限のタイムアウトオプションを設定しました。これは無駄なタイムアウトの発生を防ぎます。[[PR 1249](https://github.com/basho/riak_cs/pull/1249)]


## バグ修正
* [[Issue 1097](https://github.com/basho/riak_cs/issues/1097)/[PR 1212](https://github.com/basho/riak_cs/pull/1212)] `x-amz-metadata-directive=COPY` が指定されても、実際には Riak CS はオリジナルリソースのメタデータをコピーせずに、`REPLACE`として処理していました。`x-amz-metadata-directive=REPLACE` として扱われると、`Content-Type`は置き換わってしまいます。 `x-amz-metadata-directive` を正しく扱う処理が追加されました。
* [[Issue 1099](https://github.com/basho/riak_cs/issues/1099)/[PR 1096](https://github.com/basho/riak_cs/pull/1096)] Get Bucketリクエストで、最後のkeyが`CommonPrefixes`を含む場合に、不要なNextMarkerが返されていました。
* [[Issue 939](https://github.com/basho/riak_cs/issues/939)/[PR 1200](https://github.com/basho/riak_cs/pull/1200)] Content-Lengthヘッダーを含まないCopyリクエストは5xxエラーを返しました。この種のリクエストがCopy APIで許容されるように変更されました。更に、ゼロより大きな値がContent-Lengthに指定されたCopy APIリクエストには、明示的なエラーが返るようになりました。
* [[Issue 1143](https://github.com/basho/riak_cs/issues/1143)/[PR 1144](https://github.com/basho/riak_cs/pull/1144)] 手動でGCのバッチ起動をすると、last batch timeの表示が未来を指していました。すべての時間的なズレは修正されました。
* [[Issue PR 1162](https://github.com/basho/riak_cs/pull/1162)/[PR 1163](https://github.com/basho/riak_cs/pull/1163)] `log.syslog=on` が設定されていると、Riak CSが起動しないという、設定システムの問題が修正されました。
* [[Issue 1169](https://github.com/basho/riak_cs/issues/1169)/[PR 1200](https://github.com/basho/riak_cs/pull/1200)] PUT Copy APIの利用時に、コピー元がそもそも存在しない、もしくはリクエストユーザーがアクセス権を持たない場合、エラーレスポンスにコピー元のパスでなく、コピー先のパスが出力されていました。今ではコピー元が適切に表示されています。
* [[PR 1178](https://github.com/basho/riak_cs/pull/1178)] バケットポリシーの単一条件下で、複数IPアドレスの表現がリストとしてパースされませんでした。
* [[PR 1185](https://github.com/basho/riak_cs/pull/1185)] riak-cs.confで`proxy_get_active` がenabled、disabled以外で定義されていると、過剰なログ出力がありました。今回、`proxy_get_active` はブール値以外の表現も受け入れるようになりました。
* [[PR 1184](https://github.com/basho/riak_cs/pull/1184)] 削除プロセスがマニフェストの状態を更新する際、`put_manifest_timeout` の代わりに `put_gckey_timeout`が使われていました。
* [[Issue 1201](https://github.com/basho/riak_cs/issues/1201)/[PR 1230](https://github.com/basho/riak_cs/pull/1230)] ひとつの遅い、もしくは静かに故障したノードが断続的なユーザー取得の失敗を起こしていました。graceピリオドが追加され、`riakc_pb_socket` が再接続を試みるようになりました。
* [[PR 1232](https://github.com/basho/riak_cs/pull/1232)] プライマリリード(PR)の条件が満たされない、というwarningログが出力されていました。ユーザーはRiak CS内のオブジェクトであり、Riak CSは殆ど全てのリクエストで、認証するためにこれらのオブジェクトを参照します。これにより、ひとつでもプライマリvnodeが停止、もしくは応答できない状態にあれば、参照時のオプション(PR=all)は失敗し、ログ出力されました。Riakが高可用にセットアップされていることを考慮すると、これらのログはとても目障りなものでした。また、Riak CS 2.1 以前のログである、"No WM route" が今回復活しました。これは開発フェーズでのクライアントエラーすべてを示すので、ログの重要度(severity)はdebugへ下がりました。
* [[PR 1237](https://github.com/basho/riak_cs/pull/1237)]  `riak-cs-admin` status コマンドの実行が成功したにもかかわらず、そのexit codeがゼロではありませんでした。これはゼロを返すようになります。
* [[Issue 1097](https://github.com/basho/riak_cs/issues/1097)/[PR 1212](https://github.com/basho/riak_cs/pull/1212) および [PR 4](https://github.com/basho/s3-tests/pull/4)] Riak S2 が PUT Object Copy APIにおいて、コピー元のユーザーメタデータをコピーしない問題に対処しました。
*  [[Issue 1097](https://github.com/basho/riak_cs/issues/1097)/[PR 1212](https://github.com/basho/riak_cs/pull/1212) および [PR 4](https://github.com/basho/s3-tests/pull/4)] Multipart Complete リクエスト後に削除されるべき、アップロード済みpartへの対応がなされました。
* [[Issue 1244](https://github.com/basho/riak_cs/issues/1244)/[PR 1246](https://github.com/basho/riak_cs/pull/1246)] Riak S2 2.1.0 以前、 コピー元とコピー先が一致する PUT Copy API はContent-Typeの更新に失敗していました。Content-Typeは現在、このAPIで正しく更新されます。
* [[Issue PR 1261](https://github.com/basho/riak_cs/pull/1261), [[PR 1263](https://github.com/basho/riak_cs/pull/1263)] `riak-cs.conf` が使用されず、設定ファイルが生成されない場合でも、`app.config` で`riak-cs-debug`が動くように修正されました。


# Riak CS 2.0.1 リリースノート

## General Information
これはバグフィックスリリースです。

## バグ修正

* 設定項目 `gc.interval` に対して `infinity` を設定できないバグを修正
  ([#1125](https://github.com/basho/riak_cs/issues/1125)
  / [PR#1126](https://github.com/basho/riak_cs/pull/1126)).
* アクセスログを無効化する設定項目 `log.access` を追加
  ([#1109](https://github.com/basho/riak_cs/issues/1109)
  / [PR#1115](https://github.com/basho/riak_cs/pull/1115)).
* `riak-cs.conf` に不足していた項目 ` max_buckets_per_user` と `gc.batch_size` を追加
  ([#1109](https://github.com/basho/riak_cs/issues/1109)
  / [PR#1115](https://github.com/basho/riak_cs/pull/1115))
* XML を HTTP ボディに持つ Delete Multiple Object API とユーザ管理 API において
  連続する空白文字処理のバグを修正
  ([#1129](https://github.com/basho/riak_cs/issues/1129)
  /[PR#1135](https://github.com/basho/riak_cs/pull/1135))
* AWS v4 ヘッダ認証での URL パスリソースとクエリパラメータのバグを修正。以前の
  バージョンでは空白文字に対して `%20` ではなく `+` が使用されていた。
  ([PR#1141](https://github.com/basho/riak_cs/pull/1141))


# Riak CS 2.0.0 リリースノート

## 概要

- 本リリースは Riak 2.0.5 で動作する Riak CS へのアップデートです。
- 設定ファイルがシンプルになりました。
- 以前の Riak や Riak CS への公式パッチはすべて今回のリリースに含まれています。
  Riak CS 1.4.x や 1.5.x に対してリリースされたパッチを Riak CS 2.0.x へ適用する必要はありません。
  また、Erlang/OTP がRiak CS 2.0.x で更新されたため、以前リリースされたパッチは Riak CS 2.0.x で直接は利用できません。
- アップグレードの前にこのリリースノート全体に目を通してください。

## 既知の問題・制限事項

- なし

## 変更と追加

- `gc_max_workers` の名前を `gc.max_workers` へ変更。Riak CS クラスターの負荷軽減のため、
  デフォルト値を 5 から 2 へ下げた。
- GET Location API の部分サポート (#1057)
- AWS v4 ヘッダー認証の予備実装 - クエリ文字列認証、オブジェクトチャンク、
  ペイロードチェックサムは未実装 (#1064)。v4 認証機能の信頼性を高める残作業あり。
- 依存性グラフにエンタープライズ機能の依存を追加 (#1065)
- Cuttlefishの導入 (#1020, #1068, #1076, #1086, #1090)
  (Stanchion #88, #90, #91)
- パフォーマンス測定用 Yessir Riak client の導入 (#1072, #1083)
- インスペクターの改善と利用方法の変更 (#1084)
- S3 認証における signed date のチェック (#1067)
- `cluster_info` を含む依存ライブラリの更新 (#1087, #1088)
  (Stanchion #85, #87, #93)
- ストレージ集計の最適化 (#1089) Riak 2.1 以降で `use_2i_for_storage_calc` フラグ
  を付けるとストレージ集計のディスクリードが軽くなる可能性がある。

## バグ修正

- webmachine の誤った log handler 名を修正 (#1075)
- lagerのクラッシュを修正 (#1038)
- ハードコードされたcrashdumpのパスを修正 (#1052)
- 不要な警告メッセージを抑制 (#1053)
- Multibag の状態遷移を単純化 (Multibag #21)
- Multibag環境への移行後に GC のブロック削除が失敗する問題を修正
  (Multibag #19)
- 単一bag環境から複数bag環境への移行すると、
  すでに保存されていたオブジェクトへのアクセスでエラーが発生し、
  コネクションが切断される問題を修正 (Multibag #18).

## 廃止に関する注意

- マルチデータセンターレプリケーション v2 のサポートが終了しました。
- 古い list objects の実装 (`fold_objects_for_list_keys` が `false`) は廃止されました。
  次のメジャーバージョンでは*削除*されます。
- ページングをしない GC (`gc_paginated_indexes` が `false`) は廃止されました。
  次のメジャーバージョンでは*削除*されます。


## Riak CS 2.0.0 へのアップグレード

Riak CS システム のアップグレードはそれを構成する Riak、Riak CS、Stanchion のアップグレードが必要となります。
アップグレード手順は既存システムの設定、サブシステムのバージョンの組み合わせに依存するため、単純ではありません。
このドキュメントには一般的な手順とシステム全体をRiak CS 2.0.0へアップグレードする際の注意点を記載します。

#### 新しい設定システム

Riak 2.0.0は新しい設定システム (`riak.conf`) を導入しました。そして、
Riak CS 2.0.0 から Riak CS も新しい設定スタイルのサポートをします。Riak と Riak CS は共に
`app.config` と `vm.args` による古い設定スタイルもサポートしています。

`riak.conf`、`riak-cs.conf`、`stanchion.conf` を使った
**Basho は統合された新しい設定システムへの移行をお勧めします。**

##### 旧来の app.config 利用における注意

**旧来の `app.config` を Riak CS や Stanchion で利用する場合、
いくつかの変更されたパラメータを更新する必要があります**

特に Riak CS の `app.config` では:
- `cs_ip` と `cs_port` は `listener` へ統合されました
- `riak_ip` と `riak_pb_port` は `riak_host` へ統合されました
- `stanchion_ip` と `stanchion_port` は `stanchion_host` へ統合されました
- `admin_ip` と `admin_port` は `admin_listener` へ統合されました
- `webmachine_log_handler` は `webmachine_access_log_handler` へ変更されました


Stanchion の `app.config` では:
- `stanchion_ip` と `stanchion_port` は `listener` へ統合されました
- `riak_ip` と `riak_port` は `riak_host` へ統合されました

上記のペアはいずれも似た形式にならいます。
古い形式が別々のIPとポートのパラメータを使うのに対し、
新しい形式はこれらを `{new_option, {"IP", Port}}` のように統合します。
例えば旧来の `app.config` での設定が以前は:

```
{riak_cs, [
    {cs_ip, "127.0.0.1"},
    {cs_port, 8080 },
    . . .
]},
```

だったとすると、

これは現在、次のようになります:

```
{riak_cs, [
    {listener, {"127.0.0.1", 8080}},
    . . .
]},
```

他の設定も同様に読み替えてください。

#### Riak CS 1.5.3 以前からのアップグレードの注意点

アップグレード後の[オブジェクトキーの変更][riak_cs_1.5_release_notes_upgrading]
このバグ修正により アプリケーションに修正が必要となる場合があります。

#### Riak CS 1.5.0 以前からのアップグレードの注意点

[ユーザー毎のバケット所持数制限][riak_cs_1.5_release_notes_upgrading_1]
が1.5.1で導入されました。システム上限を緩和しない場合、
100バケット以上を持つユーザーはアップグレード後にバケット作成ができなくなります。

#### Riak CS 1.4.x からのアップグレードの注意点

[削除バケット内の完了していないマルチパートの除去][riak_cs_1.5_release_notes_incomplete_mutipart]
の実施が必要です。これを実施しない場合、過去に存在したその名前での新規バケット作成ができません。
操作は 409 Conflict で失敗してしまいます。


ガベージコレクションのタイムスタンプ管理が1.5.0で変更されたため、
アップグレード中は 猶予期間(Leeway seconds)とディスク空き容量に注意してください。
1.5 リリースノートの[猶予期間(Leeway seconds)とディスク空き容量][riak_cs_1.5_release_notes_leeway_and_disk] セクションで
より詳しい説明を確認してください。

#### Riak CS 1.3.x 以前からのアップグレードの注意点

Bashoは2つ前のメジャーバージョンから最新バージョンへのアップグレードをサポートしています。
従って1.4.x と 1.5.x からのアップグレードしかこのドキュメントではカバーされていません。

Riak CS 1.4.0より古いバージョンから Riak CS 2.0.0へアップグレードするには
はじめにシステムをRiak CS 1.4.5 もしくは 1.5.4 へアップグレードしてください。
推奨されるのは1.5.4へのアップグレードです。
配下のRiak環境もRiak 1.4.xシリーズへのアップグレードが必要です（1.4.12が好ましい）。


## アップグレード手順

#### すべてのシナリオ

他のサブシステムのアップグレード前に、まずはStanchionをアップグレードすることを推奨しています。
このとき Riak CS ノードから、同時に複数のアクティブなStanchionノードへアクセス可能にならないよう、注意してください。

Stanchionが稼働している各ノードで下記の手順を実施:

1. Stanchionを停止
2. Stanchionの全設定ファイルをバックアップ
3. 現在のStanchionパッケージをアンインストール
4. 新しいStanchion 2.0.0 パッケージをインストール
5. Stanchion の設定ファイルを移行 (下記参照)
6. Stanchionを起動

#### シナリオ: Riak CS と Riak が同じホストで稼働する場合

下記の手順を各ノードで実施:

1. Riak CSを停止
2. Riak を停止
3. Riak、Riak CS の全設定ファイルをバックアップし、全パッチファイルを削除
4. 現在のRiak CSパッケージをアンインストール
5. 現在のRiakパッケージをアンインストール
6. 新しいRiakパッケージをインストール
7. 新しいRiak CS 2.0.0 パッケージをインストール
8. Riak の設定ファイルを移行 (下記参照)
9. Riak CS の設定ファイルを移行 (下記参照)
10. Riakを起動
11. Riak CSを起動

#### Scenario: Riak CSとRiakが別々のホストで稼働する場合

Riak CSがRiakと同一ホストにインストールされていない場合、
対応するRiakノードが動いていれば Riak CS はいつでもアップグレードできます。

下記の手順を各ノードで実施:

1. Riak CSを停止
2. 全設定ファイルをバックアップし、全パッチファイルを削除
3. 現在のRiak CSパッケージをアンインストール
4. 新しいRiak CS 2.0.0 パッケージをインストール
5. Riak CS の設定ファイルを移行 (下記参照)
6. Riak CSを起動

## Stanchion 2.0.0へのアップグレード

stanchion 2.0.0へのアップグレードの際、`app.config` と `vm.args` は `stanchion.conf` へ移行されます。
設定ファイルは以前と同じ場所に置かれます。

#### Stanchion 設定の移行

下記の設定項目対応表を使い、Riak CS 1.5.x と 2.0.0 間で設定が引き継がれるように
`stanchion.conf` を編集します。

この表には新旧の設定フォーマットとデフォルト値が記載されています。

##### Stanchion app.config における `stanchion` セクション

|      1.5.4 (`app.config`)          |        2.0.0 (`stanchion.conf`)       |
|:-----------------------------------|:--------------------------------------|
|`{stanchion_ip, "127.0.0.1"}`       |`listener = 127.0.0.1:8080`            |
|`{stanchion_port, 8085}`            |                                       |
|`{riak_ip, "127.0.0.1"}`            |`riak_host = 127.0.0.1:8087`           |
|`{riak_pb_port, 8087}`              |                                       |
|`{admin_key, "admin-key"}`          |`admin.key = admin-key`                |
|`{admin_secret, "admin-secret"}`    |`admin.secret = admin-secret`          |

##### Stanchion app.config における `lager` セクション

Riak's Lager configuration can be copied directly to the `advanced.config` file.
Stanchionの Lager 設定は直接 `advanced.config` へコピー可能です。


##### Stanchion app.config における `ssl` セクション

デフォルトではコメントアウトされ、その結果 SSL無効の `undefined` になる

|      1.5.4 (`app.config`)          |        2.0.0 (`stanchion.conf`)       |
|:-----------------------------------|:--------------------------------------|
|`{ssl, [`                           |                                       |
|`  {certfile, "./etc/cert.pem"}`    |`ssl.certfile`                         |
|`  {keyfile, "./etc/key.pem"}`      |`ssl.keyfile`                          |


## Riak の 2.0.5 へのアップグレード と Riak CS 2.0.0 用設定

Riak CS 2.0.0 は Riak 2.0.5 上でのみ動作し、Riak 1.x.x 系 では*動きません*。このため、
配下のRiak環境は Riak 2.0.5 へのアップグレードが*必須*となります。ここでは Riak 1.4.x
からのアップグレードのみをカバーします。Riakのアップグレードに関する更なる情報はRiakの
[2.0へのアップグレード][upgrading_to_2.0]を参照してください。

下記はRiak 2.0.5 クラスターで Riak CS を動かすための設定です。

#### Riak のアップグレード - ステップ 1: デフォルトのバケットプロパティ設定

旧バージョンのRiakでは、デフォルトのバケットプロパティは
`app.config` で下記のように設定されました。

```erlang
{riak_core, [
   ...
   {default_bucket_props, [{allow_mult, true}]},
   ...
]}.
```

Riak 2.0.5 の `riak.conf` では次のように設定します:

```
buckets.default.allow_mult = true
```

#### Riak のアップグレード - ステップ 2: `riak_kv` 設定

RIak CS 2.0.0配下のRiak 2.0.5を設定する方法は2つあります:

**オプション 1: Riak 1.4.x の既存 `app.config` の再利用**

この場合、 Riak CS 2.0.0 パッケージでインストールされた
新しい Riak CS バイナリを `add_paths` が参照するように変更します。
これは `"/usr/lib/riak-cs/lib/riak_cs-1.5.4/ebin"` から
`"/usr/lib/riak-cs/lib/riak_cs-2.0.0/ebin"` への変更です。

**オプション 2: Riak 2.0.0 の新しい `advanced.config` を使う**

全てのriak_kvの設定項目は `app.config` から `advanced.config` へコピーする必要があります。
そして Riak CS 2.0.0 パッケージでインストールされた
新しい Riak CS バイナリを `add_paths` が参照するように変更します。
`advanced.config` は下記のようになります:

```erlang
{riak_kv, [
  {add_paths, ["/usr/lib/riak-cs/lib/riak_cs-2.0.0/ebin"]}
]}.
```

`app.config` は `advanced.config` を使う場合、削除する必要があります。

詳細は [Riakバックエンドを正しく設定する][proper_backend] を参照してください。

#### Riak のアップグレード - ステップ 3: メモリサイズの見直し


LevelDBのメモリサイズ設定のデフォルト値が変更されたため、メモリ設定は見直す必要があります。
Riak CS のメモリ使用は主にBitcaskのkeydirとLevelDBのブロックキャッシュによるものです。
これに加え、IO パフォーマンス向上のためカーネルのディスクキャッシュを考慮にいれます。
下記の式はメモリサイズ指定の際に役に立つでしょう。

- バックエンド用メモリ = (Bitcask用メモリ) + (LevelDB用メモリ)
- ストレージ用メモリ = (バックエンド用メモリ) + (カーネルキャッシュ用メモリ)


##### LevelDB ブロックキャッシュサイズ

LevelDBのメモリサイズに関連する設定は `max_open_files` から `total_leveldb_mem_percent` へ
2.0 で変更されました。これによりLevelDBが消費するメモリの総量を指定します。
デフォルトのメモリ制限が `max_open_files` への比例から、システムの物理メモリサイズの
パーセンテージへ変更されていることに注意してください。

`total_leveldb_mem_percent` の[デフォルト値 (70%)][configuring_elvevedb]は、
Bitcaskを含んだ multi バックエンドにとってかなりアグレッシブな値であるため、この値の変更を*強く推奨します*。
これは Bitcaskが keydir をメモリ上で保存し、ユースーケースによってはかなりのメモリ使用をするためです。

##### Bitcask keydir のサイジング

Bitcask は全てのキーをディスク上だけでなくメモリ上にも保存します。 Bitcask上での全キーの数と、
その平均サイズを正しく見積ることは、Bitcaskのメモリ使用量を見積るために非常に重要です。
クラスター全体におけるBitcask内のキー数の合計 `N(b)` は:

```
N(b) = N(o, size <= 1MB) + N(o, size > 1MB) * avg(o, size > 1MB) / 1MB
```

であり、

ここで `N(o, size <= 1MB)` は 1MB より小さいオブジェクトの数、
`N(o, size > 1MB)` は 1MB より大きなオブジェクトの数、
そして `avg(o, size > 1MB)` は 1MB より大きいオブジェクトの平均サイズです。
Riak CS 内のキーの数は保存されたデータのMB単位での量に関連しています。
オブジェクトの平均生存期間が猶予期間（leeway period）よりも十分に小さい場合、
ガベージコレクションを待っているオブジェクトもディスク上で生存しているとみなします。
vnode毎の実際のキーの数は`riak-admin vnode-status`の出力に含まれます。各vnodeのセクションに
`Status`という名前のアイテムがあり、これが`be_blocks`セクション内に`key_count`を
含みます。

キーの数が判明したら、[Bitcask キャパシティプランニング][bitcask_capactiy_planning]
に従い、Bitcask の keydirによるメモリ使用量を見積ります。

バケット名の長さは常に 19 バイト(`riak_cs_utils:to_bucket_name/2` 参照)、
キー名の長さは常に 20 バイトです(`riak_cs_lfs_utils:block_name/3` 参照)。
大きなオブジェクトが支配的であるとすると、平均バリューサイズは1MBに近いですが、
個別のユースケースに沿って見積るべきです。

#### Riak のアップグレード - ステップ 4: `vm.args` における変更

1.4.x 系から2.0.x系のアップグレードは [設定システムのアップグレード][upgrading_your_configuration]
に記載があります。次の一覧は Riak CS における主要な設定項目です。
`erlang.distribution_buffer_size` はデフォルトでコメントアウトされています。

| Riak 1.4                        | Riak 2.0                              |
|:--------------------------------|:--------------------------------------|
|`+zdbbl`                         |`erlang.distribution_buffer_size = 1MB`|
|`-name riak@127.0.0.1`           |`nodename = riak@127.0.0.1`            |
|`-setcookie riak`                |`distributed_cookie = riak`            |

#### Riak のアップグレード - ステップ 5: ストレージ集計

ストレージ統計が必要なシステムでは更に設定が必要です。詳細は
[ストレージ統計ドキュメント][storage_statistics]をご覧ください。

#### Riak のアップグレード - 追記事項

Bitcask のストレージフォーマットは
[重要な問題の修正][riak_2.0_release_notes_bitcask]によって Riak 2.0.x で変更されています。
アップグレード後の初回起動は暗黙のデータ変換が伴います。これは全データファイルの読込み、
書込みを意味し、通常よりも高いディスク負荷を引き起こします。
データフォーマットのアップグレード時間はBitcask内のデータ量とディスクのIOパフォーマンスに
依存します。

データ変換は下記のログと共に開始されます:

```
2015-03-17 02:43:20.813 [info] <0.609.0>@riak_kv_bitcask_backend:maybe_start_upgrade_if_bitcask_files:720 Starting upgrade to version 1.7.0 in /mnt/data/bitcask/1096126227998177188652763624537212264741949407232
2015-03-17 02:43:21.344 [info] <0.610.0>@riak_kv_bitcask_backend:maybe_start_upgrade_if_bitcask_files:720 Starting upgrade to version 1.7.0 in /mnt/data/bitcask/1278813932664540053428224228626747642198940975104
```

データ変換の完了は下記のログによって確認できます。

```
2015-03-17 07:18:49.754 [info] <0.609.0>@riak_kv_bitcask_backend:callback:446 Finished upgrading to Bitcask 1.7.0 in /mnt/data/bitcask/1096126227998177188652763624537212264741949407232
2015-03-17 07:23:07.181 [info] <0.610.0>@riak_kv_bitcask_backend:callback:446 Finished upgrading to Bitcask 1.7.0 in /mnt/data/bitcask/1278813932664540053428224228626747642198940975104
```


## Riak CS 2.0.0 へのアップグレード

#### Riak CS のアップグレード - ステップ 1: Multibag 設定

Multibag設定を Riak CS および Stanchion の `advanced.config` へ移します。

#### Riak CS のアップグレード - ステップ 2: デフォルト設定

Riak CS 2.0.0へのアップグレード時に `app.config` と `vm.args` は 単一ファイルの `riak-cs.conf`
へ移行されます。設定ファイルは以前と同じ場所へ置かれます。

注意: アップグレードが完了し、`riak-cs.conf` を使う場合、
**`app.config` は必ず削除してください**

1.5.x から 2.0.0の間で重要な設定の変更があり、全ての項目が1:1で変換されていません。

下記の設定項目対応表を使い、Riak CS 1.5.x と 2.0.0 間で設定が引き継がれるように
`riak-cs.conf` を編集します。

この表は新旧の設定フォーマットとデフォルト値が記載されています。

注意: `storage.stats.schedule.$time` はデフォルト値を持ちませんが、例として追加してあります。

##### Riak CS app.config の `riak_cs` セクション

|      1.5.4 (`app.config`)          |        2.0.0 (`riak-cs.conf`)          |
|:-----------------------------------|:---------------------------------------|
|`{cs_ip, "127.0.0.1"}`              |`listener = 127.0.0.1:8080`             |
|`{cs_port, 8080}`                   |                                        |
|`{riak_ip, "127.0.0.1"}`            |`riak_host = 127.0.0.1:8087`            |
|`{riak_pb_port, 8087}`              |                                        |
|`{stanchion_ip, "127.0.0.1"}`       |`stanchion_host = 127.0.0.1:8085`       |
|`{stanchion_port, 8085 }`           |                                        |
|`{stanchion_ssl, false }`           |`stanchion_ssl = off`                   |
|`{anonymous_user_creation, false}`  |`anonymous_user_creation = off`         |
|`{admin_key, "admin-key"}`          |`admin.key = admin-key`                 |
|`{admin_secret, "admin-secret"}`    |`admin.secret = admin-secret`           |
|`{cs_root_host, "s3.amazonaws.com"}`|`root_host = s3.amazonaws.com`          |
|`{connection_pools,[`               |                                        |
|` {request_pool, {128, 0} },`       |`pool.request.size = 128`               |
|                                    |`pool.request.overflow = 0`             |
|` {bucket_list_pool, {5, 0} }`      |`pool.list.size = 5`                    |
|                                    |`pool.list.overflow = 0`                |
|`{trust_x_forwarded_for, false}`    |`trust_x_forwarded_for = off`           |
|`{leeway_seconds, 86400}`           |`gc.leeway_period = 24h`                |
|`{gc_interval, 900}`                |`gc.interval = 15m`                     |
|`{gc_retry_interval, 21600}`        |`gc.retry_interval = 6h`                |
|`{access_log_flush_factor, 1}`      |`stats.access.flush_factor = 1`         |
|`{access_log_flush_size, 1000000}`  |`stats.access.flush_size = 1000000`     |
|`{access_archive_period, 3600}`     |`stats.access.archive_period = 1h`      |
|`{access_archiver_max_backlog, 2}`  |`stats.access.archiver.max_backlog = 2` |
|(no explicit default)               |`stats.access.archiver.max_workers = 2` |
|`{storage_schedule, []}`            |`stats.storage.schedule.$time = 0600`   |
|`{storage_archive_period, 86400}`   |`stats.storage.archive_period = 1d`     |
|`{usage_request_limit, 744}`        |`riak_cs.usage_request_limit = 31d`     |
|`{cs_version, 10300 }`              |`cs_version = 10300`                    |
|`{dtrace_support, false}`           |`dtrace = off`                          |

##### Riak CS app.config の `webmachine` セクション

|      1.5.4 (`app.config`)          |        2.0.0 (`riak-cs.conf`)         |
|:-----------------------------------|:--------------------------------------|
|`{server_name, "Riak CS"}`          |`server_name = Riak CS`                |
|`{log_handlers, ....}`              |`log.access.dir = /var/log/riak-cs`    |

アクセスログ出力を無効化するには `riak-cs.conf` の `log.access.dir` ではじまる行を
を削除するか、コメントアウトします。

WebMachineの変更により, `app.config` もしくは `advanced.config` に `log_handlers` が
定義されている場合、ログハンドラーの名前を下記のように変更します:

```erlang
    {log_handlers, [
        {webmachine_access_log_handler, ["/var/log/riak-cs"]},
        {riak_cs_access_log_handler, []}
        ]},
```

`log_handlers` が `app.config` もしくは `advanced.config` に定義されていない場合、
この変更は必要ありません。

##### Riak CS app.config の `lager` セクション

Riak CS の Lager 設定は直接 `advanced.config` へコピー可能です。


#### Riak CS のアップグレード - ステップ 3: コメントアウトされた設定

全てのコメントアウトされた設定はモジュールを除いて、定義されない、もしくは無効です。

`rewrite_module` と `auth_module` はコメントアウトされていますが、デフォルト値は
Riak CS 1.5.4 から変わっていません。このセクションでは OOS API への変更方法を紹介します。

|      1.5.4 (`app.config`)             |      2.0.0 (`riak-cs.conf`)     |
|:--------------------------------------|:--------------------------------|
|`{rewrite_module, riak_cs_s3_rewrite }`|`rewrite_module`                 |
|`{auth_module, riak_cs_s3_auth },`     |`auth_module`                    |
|`{admin_ip, "127.0.0.1"}`              |`admin.listener = 127.0.0.1:8000`|
|`{admin_port, 8000 }`                  |                                 |
|`{ssl, [`                              |                                 |
|`  {certfile, "./etc/cert.pem"}`       |`ssl.certfile`                   |
|`  {keyfile, "./etc/key.pem"}`         |`ssl.keyfile`                    |

#### Riak CS のアップグレード - ステップ 4: その他の設定

下記の設定は `riak-cs.conf` に該当する項目はありません。

* `fold_objects_for_list_keys`
* `n_val_1_get_requests`
* `gc_paginated_indexes`

これらの値が `false` に設定されている場合、Riak CS 2.0.0 の設定から除外してください。

また以前の振る舞いが好ましければ、これらを `advanced.config` の `riak_cs` セクションへ含めてください。

## Riak CS のダウングレード

### Riak CS 1.5.x へ

Riak CS 2.0.0 から Riak CS 1.5.x へ、そして Stanchion 2.0.0 から
Stanchion 1.5.x へダウングレードするには次の手順を各ノードで実施します。

1. Riak CS を停止
1. Riak を停止
1. Riak CS 2.0.0 パッケージをアンインストール
1. Riak 2.0.5 パッケージをアンインストール
1. Bitcask ダウングレードスクリプトを全Bitcaskディレクトリにて実行\*
1. 対象の Riak パッケージをインストール
1. 対象の Riak CS パッケージをインストール
1. 設定ファイルのリストア
1. Riak を起動
1. Riak CS を起動

最後にStanchionが稼働しているノードにて:

1. Stanchion の停止
2. Stanchion 2.0.0 パッケージのアンインストール
3. 対象の Stanchion パッケージをインストール
4. Stanchion の設定ファイルをリストア
5. Stanchion を起動


\*Bitcaskファイルフォーマットは Riak 1.4.x から Riak 2.0.0 の間で変更されました。
データフォーマットの暗黙アップグレードがサポートされていますが、フォーマットの自動
ダウングレードはサポートされていません。これによりダウングレードにはデータファイルの
変換スクリプトが必要です。こちらもご覧ください。[2.0 ダウングレードノート][downgrade_notes].

[riak_1.4_release_notes]: https://github.com/basho/riak/blob/1.4/RELEASE-NOTES.ja.md
[riak_2.0_release_notes]: https://github.com/basho/riak/blob/2.0/RELEASE-NOTES.ja.md
[riak_2.0_release_notes_bitcask]: https://github.com/basho/riak/blob/2.0/RELEASE-NOTES.ja.md#bitcask
[riak_cs_1.4_release_notes]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.ja.md#riak-cs-145-%E3%83%AA%E3%83%AA%E3%83%BC%E3%82%B9%E3%83%8E%E3%83%BC%E3%83%88
[riak_cs_1.5_release_notes]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.ja.md#riak-cs-154-%E3%83%AA%E3%83%AA%E3%83%BC%E3%82%B9%E3%83%8E%E3%83%BC%E3%83%88
[riak_cs_1.5_release_notes_upgrading]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.ja.md#%E3%82%A2%E3%83%83%E3%83%97%E3%82%B0%E3%83%AC%E3%83%BC%E3%83%89%E6%99%82%E3%81%AE%E6%B3%A8%E6%84%8F
[riak_cs_1.5_release_notes_upgrading_1]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.ja.md#%E3%82%A2%E3%83%83%E3%83%97%E3%82%B0%E3%83%AC%E3%83%BC%E3%83%89%E6%99%82%E3%81%AE%E6%B3%A8%E6%84%8F%E7%82%B9
[riak_cs_1.5_release_notes_incomplete_mutipart]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.ja.md#%E6%9C%AA%E5%AE%8C%E4%BA%86%E3%81%AE%E3%83%9E%E3%83%AB%E3%83%81%E3%83%91%E3%83%BC%E3%83%88%E3%82%A2%E3%83%83%E3%83%97%E3%83%AD%E3%83%BC%E3%83%89
[riak_cs_1.5_release_notes_leeway_and_disk]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.ja.md#%E3%82%AC%E3%83%99%E3%83%BC%E3%82%B8%E3%82%B3%E3%83%AC%E3%82%AF%E3%82%B7%E3%83%A7%E3%83%B3%E3%81%AE%E7%8C%B6%E4%BA%88%E6%9C%9F%E9%96%93leeway-seconds%E3%81%A8%E3%83%87%E3%82%A3%E3%82%B9%E3%82%AF%E7%A9%BA%E3%81%8D%E5%AE%B9%E9%87%8F
[upgrading_to_2.0]: http://docs.basho.com/riak/2.0.5/upgrade-v20/
[proper_backend]: http://docs.basho.com/riakcs/1.5.4/cookbooks/configuration/Configuring-Riak/#Setting-up-the-Proper-Riak-Backend
[configuring_elvevedb]: http://docs.basho.com/riak/latest/ops/advanced/backends/leveldb/#Configuring-eLevelDB
[bitcask_capactiy_planning]: http://docs.basho.com/riak/2.0.5/ops/building/planning/bitcask/
[upgrading_your_configuration]: http://docs.basho.com/riak/2.0.5/upgrade-v20/#Upgrading-Your-Configuration-System
[storage_statistics]: http://docs.basho.com/riakcs/latest/cookbooks/Usage-and-Billing-Data/#Storage-Statistics
[downgrade_notes]:  https://github.com/basho/riak/wiki/2.0-downgrade-notes
