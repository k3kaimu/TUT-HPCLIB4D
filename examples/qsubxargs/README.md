# qsubxargs

## 概要

`xargs`でコマンドを構築して，アレイジョブとして投入するコマンド

```
Usage:
    qsubxargs <tuthpc-lib options...> <xargs options...> <commands...>

Where:
    <tuthpc-lib options...>:    options for qsubxargs
    <xargs options...>:         options for xargs
    <commands...>:              commands

For example:
    ls -1 | qsubxargs --th:g=28 --th:m=100 -l echo
```


## インストール方法

以下の環境変数をbashrcなどに書く

```
export TUTHPC_CLUSTER_NAME="TUTX"
export TUTHPC_EMAIL_ADDR="<Your email address>"
export TUTHPC_QSUB_ARGS='-v SINGULARITY_IMAGE=<Image name>'
export TUTHPC_EXPORT_ENVS='USER'
export TUTHPC_DEFAULT_ARGS='--th:m=100'
```

+ `TUTHPC_EXPORT_ENVS`

`qsubxargs`実行時の環境変数のうち，ジョブでも利用する環境変数のリストをカンマ区切りで書く

+ `TUTHPC_DEFAULT_ARGS`

`qsubxargs`のデフォルト引数を**カンマ区切り**で書く．
たとえば，`--th:g=28,--th:m=4`を設定しておけば，`qsubxargs`の実行時に毎回 `--th:g=28 --th:m=4` を書かなくても良くなります．


## Tips

+ 20CPUコアを利用するジョブを最大100個投入する

以下のコマンドは，たとえxargsによってコマンドが2000（=20*100）個以上生成されたとしても，生成されるジョブを100個に制限してジョブの投入をする．
たとえば生成されたコマンドが4000個のとき，各ジョブは40個のコマンドを20並行で処理する．

```sh
qsubxargs --th:g=20 --th:m=100 <xargs options...> <commands...>
```


## ビルド方法

```sh
$ git clone git@github.com:k3kaimu/TUT-HPCLIB4D.git
$ cd TUT-HPCLIB4D/examples/qsubxargs
$ dub build
```
