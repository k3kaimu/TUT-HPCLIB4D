import std.algorithm;
import std.datetime;
import std.range;
import std.stdio;
import std.typecons;

import tuthpc.mpi;
import tuthpc.taskqueue;



void main()
{
    // 第1引数はノード数
    // 第2引数はノード当たりの起動プロセス数
    jobRun(4, 20, {
        // スケジューラ
        auto scheduler = new MPITaskScheduler();
        foreach(i; 0 .. 10){
            // 全プロセスで並列に100個の数字を表示する
            // 80ノードに対して100個の値は自動で割り振られる
            scheduler.run!(a => writeln(a[0], a[1]))(iota(100*i, 100*(i+1)).zip(repeat("times.")));
        }

        // 複数の関数を各ノードで手分けしたい場合
        // 例では，四則演算を10回ずつするタスク(計40タスク)を並列実行する
        auto list = new MultiTaskList;
        foreach(i; 0 .. 10) list.append((int a, int b) => writefln("%s + %s = %s", a, b, a + b), i, i*2);
        foreach(i; 0 .. 10) list.append((int a, int b) => writefln("%s - %s = %s", a, b, a - b), i, i*2);
        foreach(i; 0 .. 10) list.append((int a, int b) => writefln("%s * %s = %s", a, b, a * b), i, i*2);
        foreach(i; 1 .. 11) list.append((int a, int b) => writefln("%s / %s = %s", b, a, b / a), i, i*2);

        // スケジューラでlistに入っているタスクを並列実行する
        scheduler.run(list);
    });

    // 第2引数に0を指定すると，自動で最大プロセス数が選択される(wdev=20, cdev=16)
    jobRun(1, 0, {
        auto scheduler = new MPITaskScheduler();

        ulong serialMSec;
        if(scheduler.environment.isMaster){
            // Masterプロセスとは，Workerプロセスにタスクを割り振るプロセス
            writeln("I am a master process.");
            StopWatch sw;
            sw.start();
            size_t cnt;
            foreach(i; 0 .. 1000*1000*30)
                if(isPrime(i))
                    ++cnt;

            writeln(cnt);
            sw.stop();
            writeln("Serial Time: ", sw.peek.msecs, " [ms]");
            serialMSec = sw.peek.msecs;
        }else{
            // Workerプロセスとは，Masterプロセスから渡されたタスクを実行するプロセス
            writeln("I am a worker process.");
        }

        // 全プロセスでBarrierが呼ばれるまで，全プロセス待つ
        scheduler.communicator.barrier();

        StopWatch sw;
        if(scheduler.environment.isMaster) sw.start();
        ulong res;

        scheduler.run!((a, p){ *p += iota(a[0], a[1]).map!isPrime.count!"a"(); })(
            iota(0, 300).map!(a => tuple(a*100_000, (a+1)*100_000)),
            &res    // 第1引数はMasterにより各プロセスに割り振られるが，第2引数以降は各Workerプロセス固有の値となる
        );

        if(scheduler.environment.isMaster){
            sw.stop();
            writefln("Parallel time: %s[ms], %s times faster than serial", sw.peek.msecs, serialMSec*1.0 / sw.peek.msecs);
        }else{
            writeln(res);
        }
    });
}


import std.math;

pure bool isPrime(T)(T src){
    if(src <= 1)return false;
    else if(src < 4)return true;
    else if(!(src&1))return false;
    else if(((src+1)%6) && ((src-1)%6))return false;
    
    T root = cast(T)sqrt(cast(real)src) + 1;
    
    for(T i = 5; i < root; i += 6)
        if(!((src%i) && ((src)%(i+2))))
            return false;

    return true;
}
