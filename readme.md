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
    // ジョブスケジューラに投げるジョブリスト
    auto taskList = new MultiTaskList;

    // ジョブ1
    // スケジューラに投げるジョブを追加する
    taskList.append((string s){ writeln(s); }, "Hello, World!");

    // ジョブ2
    // delegateでもよいが，各ジョブは別のプロセスで実行される，
    // つまりあるプロセスでは単一のジョブのみが実行されることに注意しなければいけない．
    int a = 12;
    taskList.append((){ writeln(a); a = 100; writeln(a); });

    // ジョブをジョブスケジューラに投げる
    jobRun(taskList);

    // ここで a が 12 なのか 100 なのかはわからない．
    // なぜなら，プロセスによってはジョブ2が実行されないためである．
    // (そして，ジョブ2を実行するプロセスは1つだけである．)
    assert(a == 12 || a == 100);
}
~~~~~~

上記プログラムの実行時，各プロセスは以下のような動作を行うことでジョブを実行する．

1. クラスタマシン開発用ホストで実行されたとき，`taskList`の情報を元にしてジョブスケジューラにジョブを投げる．
2. クラスタマシンの計算ノード上で実行されたとき，`taskList`の中から自分が担当するジョブのみを実行する．

したがって，`jobRun(taskList);`を実行するまで，もしくはそれ以降のすべての処理は，並列化されず，(オーバーヘッドとして)すべてのノードで実行されてしまう．

また，`taskList`は，すべてのノード，すべてのプロセスで全く同一でなければいけない．
つまり，次のようなプログラムでは実際にはどのようなジョブが実行されるかわからない．

~~~~~d
import std.socket;
import std.stdio;
import tuthpc.taskqueue;

void main()
{
    // ジョブスケジューラに投げるジョブリスト
    auto taskList = new MultiTaskList;

    // 実行ホストのホスト名を使用する
    // 実行される計算機によってはホスト名が異なるため，
    // 以下のタスクはどのような実行結果になるか不明である．
    foreach(e; Socket.hostName)
        taskList.append((char c){ writeln(c); }, e);

    jobRun(taskList);
}
~~~~~~


### 1タスクあたり複数のMPIプロセス(1プロセス1スレッド)方式


### 1タスクあたり1プロセス(1プロセス複数スレッド)方式

未実装．
そのうち実装予定．


## MPIの使用

こんな感じで使える予定．

~~~~~~d
import tuthpc.mpi;
import tuthpc.jobqueue;


void main()
{
    // ジョブスケジューラに4ノードで実行するようにジョブを送る
    jobRun!mainJob1(4);

    // ジョブスケジューラに10ノードで実行するようにジョブを送る
    jobRun!mainJob2(10);
}


void mainJob1()
{
    // MPIの使用準備
    auto env = new MPIEnvironment();
    auto scheduler = new MPITaskScheduler(env);

    // 1000回myTask1を呼び出す
    // Masterノードは，他のWorkerノードに処理を委託する
    scheduler.run!myTask1("Hello World!".repeat(1000));

    // こういう感じでRangeを渡す
    scheduler.run!myTask2(iota(100).zip(iota(100)));
}


void mainJob2()
{
    // なんかしたいことがあれば
    ....
}


void myTask1(string msg)
{
    writeln(msg);
}


void myTask2(int a, int b)
{
    writeln(a + b);
}
~~~~~~~