import std.stdio;
import tuthpc.taskqueue;
import std.process;
import std.algorithm;
import std.string;
import std.conv;
import std.range;


immutable CMD_HEADER = "TUTHPCLIB4D:";


void printUsage()
{
    writeln(strUsage);
}


void main(string[] args)
{
    JobEnvironment env;

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
    foreach(line; pipes.stdout.byLine){
        if(line.startsWith(CMD_HEADER)){
            auto cmd = line[CMD_HEADER.length .. $];
            auto cmdargs = cmd.split(":");

            switch(cmdargs[0]) {
                case "submit":
                    pipes.stdin.writeln("-1");  // End process
                    pipes.stdin.flush();
                    writeln("Waiting...");
                    wait(pipes.pid);
                    writeln("Submit!");
                    submitToQueue(env, args, cmdargs[1].to!uint);
                    break;
                default:
                    throw new Exception("Unsupported command: %s".format(cmdargs[0]));
            }
        }
    }
}



void submitToQueue(JobEnvironment env, string[] args, uint len)
{
    import std.datetime : Clock;
    env.isEnabledRenameExeFile = false;
    env.logdir = format("logs_%s", Clock.currTime.toISOString());

    foreach(i; iota(len).runAsTasks(env)) {
        auto p = pipe();
        auto pid = spawnProcess(args, p.readEnd);
        p.writeEnd.writeln(i);
        p.writeEnd.flush();
        wait(pid);
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