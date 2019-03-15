# TUT-HPC Library for D

技科大のHPCクラスタ計算機でD言語を簡単にぶん回すためのライブラリです．


## ジョブスケジューラへのジョブ投入

### 1タスクあたり1プロセス(1スレッド)方式

次のように，ジョブスケジューラTorqueへのジョブ投入を自動化することができる．
実行されたホストがクラスタ計算機でなければ，ジョブスケジューラへの投入はせずに`std.parallelism`による並列実行を行う．

~~~~~d
import std.stdio;
import tuthpc.taskqueue;

void main()
{
    // ジョブをスケジューラに投入する際の設定
    auto env = defaultJobEnvironment();

    // ジョブスケジューラに投げるジョブリスト
    auto taskList = new MultiTaskList();

    // ジョブ1
    // スケジューラに投げるジョブを追加する
    taskList.append((string s){ writeln(s); }, "Hello, World!");

    // ジョブ2
    // delegateでもよいが，各ジョブは別のプロセスで実行される，
    // つまりあるプロセスでは単一のジョブのみが実行されることに注意しなければいけない．
    int a = 12;
    taskList.append((){ writeln(a); a = 100; writeln(a); });

    // ジョブをジョブスケジューラに投げる
    run(taskList, env);

    // ここで a が 12 なのか 100 なのかはわからない．
    // なぜなら，プロセスによってはジョブ2が実行されないためである．
    // (そして，ジョブ2を実行するプロセスは1つだけである．)
    assert(a == 12 || a == 100);
}
~~~~~~

上記プログラムの実行時，各プロセスは以下のような動作を行うことでジョブを実行する．

1. クラスタマシン開発用ホストで実行されたとき，`taskList`の情報を元にしてジョブスケジューラにジョブを投げる．
2. クラスタマシンの計算ノード上で実行されたとき，`taskList`の中から自分が担当するジョブのみを実行する．

したがって，`run(taskList, env);`を実行するまで，もしくはそれ以降のすべての処理は，並列化されず，(オーバーヘッドとして)すべてのノードで実行されてしまう．

また，`taskList`は，すべてのノード，すべてのプロセスで全く同一でなければいけない．
つまり，次のようなプログラムでは実際にはどのようなジョブが実行されるかわからない．

~~~~~d
import std.socket;
import std.stdio;
import tuthpc.taskqueue;

void main()
{
    // ジョブをスケジューラに投入する際の設定
    auto env = defaultJobEnvironment();

    // ジョブスケジューラに投げるジョブリスト
    auto taskList = new MultiTaskList();

    // 実行ホストのホスト名を使用する
    // 実行される計算機によってはホスト名が異なるため，
    // 以下のタスクはどのような実行結果になるか不明である．
    foreach(e; Socket.hostName)
        taskList.append((char c){ writeln(c); }, e);

    run(taskList, env);
}
~~~~~~


### 1タスクあたり1プロセス(1プロセス複数スレッド)方式

未実装．
そのうち実装予定．


## MPIの使用

MPIはサポートしていません．

## JobEnvironment

+ `JobEnvironment.useArrayJob`

    * `bool`
    * デフォルト値：`true`
    * アレイジョブにする場合`true`，個別のジョブを入れる場合は`false`を指定します．現在は`false`でもアレイジョブを投入するようになっています．

+ `JobEnvironment.scriptPath`

    * `string`
    * デフォルト値：`null`
    * ジョブ投入用のスクリプトファイルの名前やパスを設定できます．`null`の場合は，`qsub`の標準入力にジョブスクリプトを流し込みます．

+ `JobEnvironment.queueName`

    * `string`
    * デフォルト値：`null`
    * 実行時引数：`--th:q`, `--th:queue`, (ex. `--th:q=wEduq`)
    * ジョブを投入するキュー名を設定できます．`null`の場合は，自動で研究用キュー`wLrchq`が設定されます．

+ `JobEnvironment.unloadModules`

    * `string[]`
    * デフォルト値：`null`
    * ジョブを実行する前に`module unload`するもののリストを設定できます．

+ `JobEnvironment.loadModules`

    * `string[]`
    * デフォルト値：`null`
    * ジョブを実行する前に`module load`するもののリストを設定できます．たとえば，`matlab`などの読み込みにつかいます．

+ `JobEnbvironment.envs`

    * `string[string]`
    * デフォルト値：`null`
    * ジョブを実行する前に設定する環境変数を指定できます．

+ `JobEnvironment.isEnabledRenameExeFile`

    * `bool`
    * デフォルト値：`true`
    * ジョブをキューに投入する前に，実行ファイルの別名コピーを作成するか設定します．`true`ではコピーを作成します．コピー後のファイル名は，実行ファイルのCRC32の値にもとづき設定されます．この機能は，同一のソースコードでコンパイル時定数を変更してジョブを複数投入する場合に有効です．

+ `JobEnvironment.originalExeName`

    * `string`
    * デフォルト値：`null`
    * 実行ファイルの名前を指定します．デフォルトでは，`Runtime.args[0]`が設定されます．

+ `JobEnvironment.renamedExeName`

    * `string`
    * デフォルト値：`null`
    * リネーム後の実行ファイルの名前を指定できます．`null`の場合，実行ファイルのCRC32値にもとづいて設定されます．

+ `JobEnvironment.prescript`

    * `string[]`
    * デフォルト値：`null`
    * ジョブの実行ファイルを実行する前に，前処理を行うシェルスクリプトを指定できます．

+ `JobEnvironment.jobScript`

    * `string[]`
    * デフォルト値：`null`
    * ジョブで実行するスクリプトを指定できます．`null`のとき，`JobEnvironment.originalExeName`もしくは`JobEnvironment.renamedExeName`が実行されます．つまり，これらの値を`foo`とすると，`./foo`を実行します．

+ `JobEnvironment.postScript`

    * `string[]`
    * デフォルト値：`null`
    * ジョブの終了前に，後処理を行うシェルスクリプトを指定できます．

+ `JobEnvironment.isEnabledTimeCommand`

    * `bool`
    * デフォルト値：`false`
    * `JobEnvironment.jobScript`が`null`のとき，`JobEnvironment.originalExeName`もしくは`JobEnvironment.renamedExeName`を実行する際に`time`コマンドで時間を計測するか設定できます．デフォルト値`false`では計測しません．

+ `JobEnvironment.taskGroupSize`
    
    * `uint`
    * デフォルト値：`0`
    * 実行時引数：`--th:g`, `--th:taskGroupSize`, (ex. `--th:g=7`)
    * `nodes=1:ppn=1`のとき，複数のタスクをまとめて一つのジョブにして投入します．このとき，ppn値をこの値に設定し，この値の個数だけ並列でタスクを実行します．`nodes=1:ppn=1`で，さらにこの値が`0`のとき，この値は`11`として処理されます．この機能は，アレイジョブのジョブ数削減，及びクラスタ計算機の資源を占有しないために存在しています．クラスタ計算機の資源を占有しないためには，この値を`7`〜`14`程度の値にすることが望ましいです．

+ `JobEnvironment.ppn`

    * `uint`
    * デフォルト値：`1`
    * 実行時引数：`--th:p`, `--th:ppn`, (ex. `--th:ppn=2`)
    * 1つのジョブが実行される各ノードで何CPU使用するか指定できます．

+ `JobEnvironment.nodes`

    * `uint`
    * デフォルト値：`1`
    * 実行時引数：`--th:n`, `--th:node`, (ex. `--th:node=2`)
    * 1つのジョブを実行するノード数を指定できます．

+ `JobEnvironment.mem`, `JobEnvironment.pmem`, `JobEnvironment.vmem`, `JobEnvironment.pvmem`

    * `int`
    * デフォルト値：`-1`
    * メモリ使用量を設定できます．`-1`のときは設定されません．`0`のときは，ノードのCPU使用率が高くなるように最大メモリ量とppn値から計算されます．

+ `JobEnvironment.isEnabledEmailOnError`, `JobEnvironment.isEnabledEmailOnStart`, `JobEnvironment.isEnabledEmailOnFinish`

    * `bool`
    * デフォルト値：`false`
    * 実行時引数(`JobEnvironment.isEnabledEmailOnError`)：`--th:mailOnError`, `--th:me`
    * 実行時引数(`JobEnvironment.isEnabledEmailOnStart`)：`--th:mailOnStart`, `--th:ms`
    * 実行時引数(`JobEnvironment.isEnabledEmailOnFinish`)：`--th:mailOnFinish`, `--th:mf`
    * ジョブのエラー時，実行開始時，実行終了時にメールを送るかどうか設定できます．

+ `JobEnvironment.emailAddrs`

    * `string[]`
    * デフォルト値：`null`
    * メールを送る対象のメールアドレスを設定できます．`null`のときは，`{username}@edu.tut.ac.jp`に送られます．

+ `JobEnvironment.maxArraySize`

    * `uint`
    * デフォルト値: `8192`
    * 実行時引数： `--th:m`, `--th:maxArraySize=`, (ex. `--th:m=20`)
    * アレイジョブにおける最大のジョブ数を指定します．この数以上のジョブを投入しようとしたとき，1つのジョブで複数のタスクを実行することで，自動的にジョブ数がこの値に収まるように調整されます．

+ `JobEnvironment.maxSlotSize`

    * `uint`
    * デフォルト値： `0`
    * 実行時引数： `--th:s`, `--th:maxSlotSize=`, (ex. `--th:s=2`)
    * アレイジョブにおいて，クラスタ計算機全体で最大で同時実行されるジョブ数を制限します．この値は`0`であれば同時実行数は制限されません．計算資源の占有を回避するために利用します．

+ `JobEnvironment.isEnabledQueueOverflowProtection`

    * `bool`
    * デフォルト値：`true`
    * 実行時引数： `--th:queueOverflowProtection`, `--th:qop`, (ex. `--th:qop=false`)
    * クラスタのキューには16384個以上のジョブを管理できない制限があります．この制限を超えてジョブを投げることがないように，ジョブ投入の前にチェックするかどうかを指定できます．デフォルト値ではチェックします．

+ `JobEnvironment.isEnabledUserCheckBeforePush`

    * `bool`
    * デフォルト値：`true`
    * 実行時引数： `--th:requireUserCheck`, (ex. `--th:requireUserCheck=false`)
    * クラスタにジョブを投げる前にユーザーに確認を取るか指定します．デフォルト値ではユーザーに確認します．

+ `JobEnvironment.isForcedCommandLineArgs`

    * `bool`
    * デフォルト値：`true`
    * `--th:forceCommandLineArgs`, (ex. `--th:forceCommandLineArgs=false`)
    * 実行時に変更された`JobEnvironment`の値をコマンドライン引数で指定した値に上書きします．デフォルト値では上書きします．

+ `JobEnvironment.logdir`
    
    * `string`
    * デフォルト値: `null`
    * 実行時引数: `--th:logdir`, (ex. `--th:logdir=log_dirs`)
    * 各タスクの標準出力や標準エラーが格納されるディレクトリ名を指定します．指定されない場合，実行プログラムのハッシュ値によってディレクトリ名が決まります．このディレクトリ名はシステム上に存在しないディレクトリ名でなければいけません．
