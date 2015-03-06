# Riak CS 1.5.4 リリースノート

## 修正されたバグ

- バックプレッシャーのスリープ発動後に取得済みのRiakオブジェクトを破棄
  [riak_cs/#1041](https://github.com/basho/riak_cs/pull/1041)。
  これは次の場合に起こり得る Sibling の増加を防ぎます。
  (a) 高い同時実行アップロードによるバックプレッシャーが起動しており、かつ
  (b) バックプレッシャーによるスリープ中にアップロードがインターリーブするとき。
  この問題はマルチパートアップロードへは影響しません。
- 不要なURLデコードを引き起こす S3 API における不正確なURLパスの rewrite 処理。
  [riak_cs/#1040](https://github.com/basho/riak_cs/pull/1040).
  URLエンコード・デコードが不正確な事により、
  `%[0-9a-fA-F][0-9a-fA-F]` (正規表現) や `+` を含むオブジェクト名は
  誤ったデコードが実施されていました。この結果、前者は異なるバイナリへ、
  後者は ` ` (空白) へと置き換わり、どちらの場合でも暗黙的にデータを
  上書きする可能性があります。例えば後者のケースでは、 キー に `+` を含む
  オブジェクト(例：`foo+bar`) は、`+` が ` ` に置き換わっただけの、
  ほぼ同じ名前のオブジェクト(`foo bar`)に上書きされます。逆も起こり得ます。
  この修正は次の問題にも関連します：
  [riak_cs/#910](https://github.com/basho/riak_cs/pull/910)
  [riak_cs/#977](https://github.com/basho/riak_cs/pull/977).

## アップグレード時の注意

Riak CS 1.5.4 へアップグレードすると、デフォルト設定のままでは、
キーに `%[0-9a-fA-F][0-9a-fA-F]` や `+` を含むオブジェクトは見えなくなり、
違うオブジェクト名で見えるようになります。
前者は余分にデコードされたオブジェクトとして参照され、
後者は ` ` を `+` で置き換えたキー(例： `foo bar`)で参照されるようになります。

下記の表はアップグレードの前後で
`%[0-9a-fA-F][0-9a-fA-F]` を含むURLがどう振る舞うかの例です。


             | アップグレード前         | アップグレード後  |
:------------|:-------------------------|:------------------|
  書き込み時 | `a%2Fkey`                |          -        |
  読み込み時 | `a%2Fkey` または `a/key` | `a/key`           |
リスト表示時 | `a/key`                  | `a/key`           |

`+` や ` ` を含むオブジェクトのアップグレード前後の例：

             | アップグレード前         | アップグレード後  |
:------------|:-------------------------|:------------------|
  書き込み時 | `a+key`                  |          -        |
  読み込み時 | `a+key` または `a key`   | `a key`           |
リスト表示時 | `a key`                  | `a key`           |

             | アップグレード前         | アップグレード後  |
:------------|:-------------------------|:------------------|
  書き込み時 | `a key`                  |          -        |
  読み込み時 | `a+key` または `a key`   | `a key`           |
リスト表示時 | `a key`                  | `a key`           |

またこの修正によりアクセスログのフォーマットも単一のURLエンコードから二重エンコードスタイルへ変わります。
下記は変更前の例です：

```
127.0.0.1 - - [07/Jan/2015:08:27:07 +0000] "PUT /buckets/test/objects/path1%2Fpath2%2Fte%2Bst.txt HTTP/1.1" 200 0 "" ""
```

そしてこちらが新しいフォーマットです。

```
127.0.0.1 - - [07/Jan/2015:08:27:07 +0000] "PUT /buckets/test/objects/path1%2Fpath2%2Fte%252Bst.txt HTTP/1.1" 200 0 "" ""
```

この例から分かるように、オブジェクトのパスが `path1%2Fpath2%2Fte%252Bst.txt`
から `path1%2Fpath2%2Fte%2Bst.txt` へ変わることに注意して下さい。

もし Riak CS を利用するアプリケーション側の都合で
以前の挙動のままにしたい場合、アップグレード時に
Riak CSの設定を変更すればそれが可能です。
この場合、`rewrite_module` 設定を下記のように変更してください：

```erlang
{riak_cs, [
    %% Other settings
    {rewrite_module, riak_cs_s3_rewrite_legacy},
    %% Other settings
]}
```

**注意**: 以前の挙動は技術的に不適切であり、
前述したように暗黙的なデータの上書きが起こり得ます。
注意の上でご利用下さい。

# Riak CS 1.5.3 リリースノート

## 新規追加

- read_before_last_manifest_writeオプションの追加。
  一部のkeyへの高頻度かつ多並列でのアクセスによるSibling explosionの回避に有効。
   [riak_cs/#1011](https://github.com/basho/riak_cs/pull/1011)
- タイムアウト設定の追加。Riak - Riak CS 間の全アクセスに対してタイムアウトを設定可能にし、運用に柔軟性を提供。
  [riak_cs/#1021](https://github.com/basho/riak_cs/pull/1021)

## 修正されたバグ

- ストレージ統計の集計結果に削除済バケットのデータが含まれ得る問題を修正。
   [riak_cs/#996](https://github.com/basho/riak_cs/pull/996)

# Riak CS 1.5.2 リリースノート

## 新規追加

- Riakに対する接続失敗に関するロギングの改善
  [riak_cs/#987](https://github.com/basho/riak_cs/pull/987).
- Riakに対してアクセス統計情報の保存に失敗した際のログを追加
  [riak_cs/#988](https://github.com/basho/riak_cs/pull/988). 
  これは一時的な Riak - Riak CS 間の接続エラーによるアクセス統計ログの消失を防ぎます。
  アクセスログは `console.log` へ `warning` レベルで保存されます。
- 不正なガベージコレクション manifest の修復スクリプトの追加
  [riak_cs/#983](https://github.com/basho/riak_cs/pull/983)。
  active manifest が GCバケットへ保存される際に
  [既知の問題](https://github.com/basho/riak_cs/issues/827) があります。
  このスクリプトは不正な状態を正常な状態へ変更します。

## 修正されたバグ

- プロトコルバッファのコネクションプール (`pbc_pool_master`) のリークを修正
  [riak_cs/#986](https://github.com/basho/riak_cs/pull/986) 。
  存在しないバケットに対する認証ヘッダ無しのリクエストや、ユーザ一覧のリクエストが
  コネクションプールのリークを引き起こし、プールは結果的に空になります。このバグは1.5.0から含まれます。

# Riak CS 1.5.1 リリースノート

## 新規追加

- Sibling Explosionを避けるために sleep-after-update を追加 [riak_cs/#959](https://github.com/basho/riak_cs/pull/959)
- `riak-cs-debug` の multibag サポート [riak_cs/#930](https://github.com/basho/riak_cs/pull/930)
- Riak CS におけるバケット数に上限を追加 [riak_cs/#950](https://github.com/basho/riak_cs/pull/950)
- バケットの衝突解決を効率化 [riak_cs/#951](https://github.com/basho/riak_cs/pull/951)

## 修正されたバグ

- `riak_cs_delete_fsm` のデッドロックによるGCの停止 [riak_cs/#949](https://github.com/basho/riak_cs/pull/949)
- `riak-cs-debug` がログを収集するディレクトリのパスを修正 [riak_cs/#953](https://github.com/basho/riak_cs/pull/953)
- DST-awareなローカルタイムからGMTへの変換を回避 [riak_cs/#954](https://github.com/basho/riak_cs/pull/954)
- Secretの代わりに UUID をカノニカルID生成時のシードに利用 [riak_cs/#956](https://github.com/basho/riak_cs/pull/956)
- マルチパートアップロードにおけるパート数の上限を追加 [riak_cs/#957](https://github.com/basho/riak_cs/pull/957)
- タイムアウトをデフォルトの 5000ms から無限に設定 [riak_cs/#963](https://github.com/basho/riak_cs/pull/963)
- GC バケット内の無効な状態のマニフェストをスキップ [riak_cs/#964](https://github.com/basho/riak_cs/pull/964)

## アップグレード時の注意点

### ユーザー毎のバケット数

Riak CS 1.5.1 を使うと、ユーザーが作ることのできるバケット数を制限することができます。
デフォルトでこの最大値は 100 です。この制限はユーザーの新たなバケット作成を禁止しますが、
既に制限数を超えているユーザーが実施する、バケット削除を含む他の操作へは影響しません。
デフォルトの制限を変更するには `app.config` の `riak_cs` セクションで次の箇所を変更してください:

```erlang
{riak_cs, [
    %% ...
    {max_buckets_per_user, 5000},
    %% ...
    ]}
```

この制限を利用しない場合は  `max_buckets_per_user` を `unlimited` に設定してください。

# Riak CS 1.5.0 リリースノート

## 新規追加

* `cluster-info` 取得を含む新規コマンド `riak-cs-debug` を追加 [riak_cs/#769](https://github.com/basho/riak_cs/pull/769), [riak_cs/#832](https://github.com/basho/riak_cs/pull/832)
* 既存コマンド群を新規コマンド `riak-cs-admin` へ統合 [riak_cs/#839](https://github.com/basho/riak_cs/pull/839)
* Stanchion の IP、ポートを変更する新規コマンド `riak-cs-admin stanchion` を追加 [riak_cs/#657](https://github.com/basho/riak_cs/pull/657)
* 並行 GC によるガベージコレクション性能の向上 [riak_cs/#830](https://github.com/basho/riak_cs/pull/830)
* Iterator refresh [riak_cs/#805](https://github.com/basho/riak_cs/pull/805)
* `fold_objects_for_list_keys` 設定をデフォルト有効に変更 [riak_cs/#737](https://github.com/basho/riak_cs/pull/737), [riak_cs/#785](https://github.com/basho/riak_cs/pull/785)
* Cache-Control ヘッダーのサポートを追加 [riak_cs/#821](https://github.com/basho/riak_cs/pull/821)
* 猶予期間(`leeway_seconds`)内でもオブジェクトをガベージコレクション可能にする変更 [riak_cs/#470](https://github.com/basho/riak_cs/pull/470)
* オブジェクト、マルチパートともに PUT Copy API を追加 [riak_cs/#548](https://github.com/basho/riak_cs/pull/548)
* lager 2.0.3 へ更新
* R16B0x をビルド環境に追加 (リリースは R15B01 でビルド)
* `gc_paginated_index` 設定をデフォルト有効に変更 [riak_cs/#881](https://github.com/basho/riak_cs/issues/881)
* 新規 API: Delete Multiple Objects の追加[riak_cs/#728](https://github.com/basho/riak_cs/pull/728)
* マニフェストに対して siblings, バイト、履歴の肥大化を警告するログ追加 [riak_cs/#915](https://github.com/basho/riak_cs/pull/915)

## 修正されたバグ

* `ERL_MAX_PORTS` を Riak のデフォルトに合わせ 64000 へ変更 [riak_cs/#636](https://github.com/basho/riak_cs/pull/636)
* Riak CS 管理リソースを OpenStack API でも利用可能にする修正 [riak_cs/#666](https://github.com/basho/riak_cs/pull/666)
* Solaris でのソースビルドのバグ修正のため、パス代入コードの変更 [riak_cs/#733](https://github.com/basho/riak_cs/pull/733)
* `riakc_pb_socket` エラー時の `sanity_check(true,false)` バグを修正 [riak_cs/#683](https://github.com/basho/riak_cs/pull/683)
* Riak-CS-GC のスケジューラタイムスタンプが 2013 ではなく 0043 になるバグを修正 [riak_cs/#713](https://github.com/basho/riak_cs/pull/713) fixed by [riak_cs/#676](https://github.com/basho/riak_cs/pull/676)
* OTP code_server プロセスを過剰に呼び出すバグを修正 [riak_cs/#675](https://github.com/basho/riak_cs/pull/675)
* content-md5 が一致しない場合に HTTP 400 を返すよう修正 [riak_cs/#596](https://github.com/basho/riak_cs/pull/596)
* `/riak-cs/stats` が `admin_auth_enabled=false` の時に動作しないバグを修正. [riak_cs/#719](https://github.com/basho/riak_cs/pull/719)
* ストレージ計算で tombstone および undefined の manifest.props を処理できないバグを修正 [riak_cs/#849](https://github.com/basho/riak_cs/pull/849)
* 未完了のマルチパートオブジェクトが、バケットの削除、作成後にも残るバグを修正 [riak_cs/#857](https://github.com/basho/riak_cs/pull/857) and [stanchion/#78](https://github.com/basho/stanchion/pull/78)
* list multipart upload の空クエリパラメータの扱いを修正 [riak_cs/#843](https://github.com/basho/riak_cs/pull/843)
* PUT Object 時にヘッダ指定の ACL が設定されないバグを修正 [riak_cs/#631](https://github.com/basho/riak_cs/pull/631)
* ping リクエストの poolboy タイムアウト処理を改善 [riak_cs/#763](https://github.com/basho/riak_cs/pull/763)
* 匿名アクセス時の不要なログを削除 [riak_cs/#876](https://github.com/basho/riak_cs/issues/876)
* マルチパートでアップロードされたオブジェクトの ETag 不正を修正 [riak_cs/#855](https://github.com/basho/riak_cs/issues/855)
* PUT Bucket Policy のポリシーバージョン確認の不具合を修正[riak_cs/#911](https://github.com/basho/riak_cs/issues/911)
* コマンド成功時に終了コード 0 を返すよう修正 [riak_cs/#908](https://github.com/basho/riak_cs/issues/908)
* `{error, disconnected}` が内部で notfound に書き換えられる問題を修正 [riak_cs/#929](https://github.com/basho/riak_cs/issues/929)

## アップグレードに関する注意事項

### Riak Version

このリリースは Riak 1.4.10 上でテストされました。
[互換性マトリクス](http://docs.basho.com/riakcs/latest/cookbooks/Version-Compatibility/)
を参考に、正しいバージョンを使用していることを確認してください。

### 未完了のマルチパートアップロード

[riak_cs/#475](https://github.com/basho/riak_cs/issues/475) はセキュリティ
に関する問題で、以前に作られた同名のバケットに
対する未完了のマルチパートアップロードが、新しく作成されたバケットに
含まれてしまう可能性があります。これは次のように修正されました。

- バケット作成時には、有効なマルチパートが存在するかを確認し、
  存在する場合には 500 エラーをクライアントに返します。

- バケット削除時には、まず存在する有効なマルチパートの削除を試みた後に、
  有効なマルチパートが存在するかを(Stanchion 上で)再度確認します。
  存在する場合には 409 エラーをクライアントに返します。

1.4.x (またはそれより前のバージョン)から 1.5.0 へのアップグレード後には
いくつかの操作が必要です。

- すべてのバケットを正常な状態にするため、 `riak-cs-admin
  cleanup-orphan-multipart` を実行します。マルチパートアップロードとバ
  ケット削除が競合したときに発生しうるコーナーケースを避けるために、こ
  のコマンドは `2014-07-30T11:09:30.000Z`のような、 ISO 8601 形式の日付
  を引数として指定することができます。この引数があるとき、バケットのク
  リーンアップ操作はそのタイムスタンプよりも新しいマルチパートアップロー
  ドを削除しません。もしこれを指定する場合は、全てのCSノードのアップグ
  レードが終わって以降の時間がよいでしょう。

- 上記操作が終了するまでの期間は、削除済みのバケットで、未完了のマルチ
  パートアップロードを含むバケットは再作成が出来ない場合があります。こ
  のような再作成の失敗は [critical] ログ (`"Multipart upload remains
  in deleted bucket <bucketname>"`) で確認可能です。

### ガベージコレクションの猶予期間(Leeway seconds)とディスク空き容量

[riak_cs/#470](https://github.com/basho/riak_cs/pull/470) は、
オブジェクト削除とガベージコレクションの振る舞いを次のように変更します。
これまで、ガベージコレクションバケットのタイムスタンプはオブジェクトが
回収される将来の時刻でしたが、削除された時刻そのものへと変わります。
同時に、ガベージコレクターは現在の時刻までのタイムスタンプを回収していましたが、
猶予期間(`leeway_seconds`)だけ過去のタイムスタンプまでだけを回収するようになります。

以前(- 1.4.x):

```
           t1                         t2
-----------+--------------------------+------------------->
           DELETE object:             GC 実行:
           "t1 + leeway"              "t2" までの
           とマークされる             オブジェクトを回収
```

今後(1.5.0-):

```
           t1                         t2
-----------+--------------------------+------------------->
           DELETE object:             GC 実行:
           "t1"                       "t2 - leeway" までの
           とマークされる             オブジェクトを回収
```

これにより、1.5.0 へのアップグレード直後(仮に`t0`とします）にはオブジェ
クトが回収されない期間ができます。つまり `t0` から `t0 + leeway` までの
期間です。そして `t0` 直前に削除されたオブジェクトは `t0 + 2 * leeway`
時点で回収可能になります。

ローリングアップグレードに際しては、GC を実行している CS ノードを
**最初に** アップグレードする必要があります。
GC を実行しない CS ノードは、猶予期間が正しく動作するために、その後から
アップグレードして下さい。
また、`riak-cs-admin gc set-interval infinity` コマンドを実行して
ガベージコレクションを無効にしておくと、ノードの順序を
気にすることなくアップグレードが可能です。

マルチデータセンター構成のクラスタは、より慎重になる必要があります。
ガベージコレクションを確実に無効化してからアップグレードしてください。

## 既知の問題と制限事項

* コピーを実行中にクライアントが次のリクエストを送信するとコピーは中断
  されます。これはクライアントの切断を検出してコピーを中止する機構の副
  作用です。詳しくは [#932](https://github.com/basho/riak_cs/pull/932)
  をご覧ください。

* OOSインターフェースでのコピーはサポートされていません。

* Multibag はオブジェクトのマニフェストとブロックを複数の異なるクラスタ
  に分けて格納する機能です。これは Riak CS Enterprise の機能として追加
  されましたが、技術プレビューの段階にあります。クラスタ間レプリケーショ
  ンによる `proxy_get` はサポートされておりません。Multibagは今のところ、
  ひとつのDCでのみ動作するように設計されています。

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
