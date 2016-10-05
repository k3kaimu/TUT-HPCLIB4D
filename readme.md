# TUT-HPC Library for D

技科大のHPCクラスタ計算機でD言語を簡単にぶん回すためのライブラリです．


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
    auto env = new MPIEnvironment()
    auto scheduler = MPITaskSchedular(env);

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