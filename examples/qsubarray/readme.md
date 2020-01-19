# qsubarray

## 概要

アレイジョブに特化したジョブ投入コマンド


## インストール方法

PATHの通った適当なディレクトリに，ビルド済みバイナリをダウンロードして入れておく．

```sh
$ cd ~/local/bin
$ wget https://github.com/k3kaimu/cluster-tools/releases/latest/download/qsubxargs
$ chmod +x qsubxargs
```

以下の環境変数をbashrcなどに書く．

```
export TUTHPC_CLUSTER_NAME="TUTX"
export TUTHPC_QSUB_ARGS='-v SINGULARITY_IMAGE=<Image name>'
export TUTHPC_EXPORT_ENVS='USER'
export TUTHPC_DEFAULT_ARGS='--th:m=100'
export TUTHPC_STARTUP_SCRIPT='source ~/.bashrc'
```

+ `TUTHPC_EXPORT_ENVS`

`qsubxargs`実行時の環境変数のうち，ジョブでも利用する環境変数のリストをカンマ区切りで書く

+ `TUTHPC_DEFAULT_ARGS`

`qsubxargs`のデフォルト引数を**カンマ区切り**で書く．
たとえば，`--th:g=28,--th:m=4`を設定しておけば，`qsubxargs`の実行時に毎回 `--th:g=28 --th:m=4` を書かなくても良くなります．


+ `TUTHPC_STARTUP_SCRIPT`

各ジョブが計算ノードで実行されるときに，事前に実行されるシェルスクリプト．
環境変数などの設定などの用途に使用することを想定しています．


## 使い方

Pythonを例にして具体的な使い方を説明する．
まず，クラスタ計算機に投入する処理は次のような処理とする．

```python
for i in range(100):
	# この処理を100個のジョブとして投入したい
	print("Hello, world! This is {0}th task.".format(i))
```

まず，実行したいプログラムから標準出力にどれだけのジョブを投げるのかを出力する．
たとえば，今回は100個のジョブを投げるため，以下のように書く．
ただし，行末には必ず改行をいれること．

```python
print("TUTHPCLIB4D:submit:100")
```

すると，標準入力経由で整数値が一つプログラムに渡される．
直前に標準出力に出力したジョブの個数を`N`とすると，この整数値は`0`から`N-1`までの間の整数値となる．
プログラムではこの整数値をループ変数の代わりとして利用する．

```python
index = int(input())
print("Hello, world! This is {0}th task.".format(index))
```

より実践的には次のPythonの関数`submitJob`を見ると良い．
`submitJob`は関数オブジェクトのリスト`taskList`を引数に取り，それをジョブとして投入する．
また，ジョブの実行時には該当するタスクを実行する．

```python
def submitJob(taskList):
    # qsubarrayにタスクの数を伝える
    print("TUTHPCLIB4D:submit:{0}".format(len(taskList)))

    # qsubarrayからこのプロセスが実行すべきタスクのインデックスを得る
    index = int(input())

    # 該当するindexのものを実行
    if index >= 0 and index < len(taskList):
        taskList[index]();
```

この`submitJob`は次のように使う．

```python
# 投入するタスク
def makeTask(i):
    def task():
        print("Hello, world!: {0}".format(i))

    return task

# ジョブとして投入するタスクのリストを生成する
taskList = []
for i in range(10):
    taskList.append(makeTask(i))

submitJob(taskList)
```

## Tips

+ 複数のノードを専有したいとき

以下のコマンドで1ノードあたり20CPUで4つのノードを専有する．

```sh
qsubarray --th:m=4 --th:g=20 <commands...>
```

+ 1つのジョブが複数のCPUを専有するとき

OpenMPなどのときに使用．

```sh
qsubarray --th:ppn=10 <commands...>
```
