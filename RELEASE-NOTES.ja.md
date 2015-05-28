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
