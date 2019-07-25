module app;

import core.thread;

import std.algorithm;
import std.conv;
import std.datetime;
import std.process;
import std.range;
import std.stdio;
import std.string;

import tuthpc.taskqueue;

immutable CMD_HEADER = "TUTHPCLIB4D:";


void printUsage()
{
    writeln(strUsage);
}


void main(string[] args)
{
    auto env = defaultJobEnvironment;
    immutable startUpTime = Clock.currTime;

    if(args.length == 1) {
        printUsage();
        return;
    }

    // プログラム名を無視
    args = args[1 .. $];

    // "-"が頭についている引数は無視する
    while(args.length && args[0].startsWith("-"))
        args = args[1 .. $];

    if(args.length == 0) {
        printUsage();
        return;
    }

    // プログラムを起動
    auto pipes = pipeProcess(args);
    ulong submitCount = 0;
    foreach(line; pipes.stdout.byLine){
        if(line.startsWith(CMD_HEADER)){
            auto cmd = line[CMD_HEADER.length .. $];
            auto cmdargs = cmd.split(":");

            switch(cmdargs[0]) {
                case "submit":
                    submitToQueue(env, args, cmdargs[1].to!ulong, startUpTime, submitCount);
                    ++submitCount;
                    pipes.stdin.writeln("-1");  // 次のコマンドに移る
                    pipes.stdin.flush();
                    Thread.sleep(1.seconds);
                    break;
                default:
                    throw new Exception("Unsupported command: %s".format(cmdargs[0]));
            }
        }
    }

    foreach(line; pipes.stderr.byLine)
        stderr.writeln(line);

    writeln("Waiting process termination...");
    wait(pipes.pid);
}



void submitToQueue(JobEnvironment env, string[] args, ulong len, SysTime time, ulong runCount)
{
    env.isEnabledRenameExeFile = false;
    env.logdir = format("logs_%s_%s", time.toISOString(), runCount);

    foreach(i; iota(len).runAsTasks(env)) {
        auto p = pipe();
        auto pid = spawnProcess(args, p.readEnd);
        scope(failure) kill(pid);
        scope(success) wait(pid);

        // 該当するsubmitが出現するまで-1を与える
        foreach(_; 0 .. runCount) {
            p.writeEnd.writeln("-1");
            p.writeEnd.flush();
        }

        // 該当するsubmitに対してタスクの番号を与える
        p.writeEnd.writeln(i);
        p.writeEnd.flush();
        //wait(pid);

        while(!tryWait(pid).terminated) {
            p.writeEnd.writeln("-1");
            p.writeEnd.flush();

            Thread.sleep(100.msecs);
        }
    }
}


immutable string strUsage = `
Usage:
    qsubarray <options...> <commands...>

Where:
    <options...>:   list of options for qsubarray
    <commands...>:  commands

For example:
    qsubarray --th:g=20 --th:m=4 python script.py
`;