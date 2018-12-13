import std.stdio;
import tuthpc.taskqueue;
import std.process;
import std.algorithm;
import std.string;
import std.conv;
import std.range;


immutable CMD_HEADER = "TUTHPCLIB4D:";

void main(string[] args)
{
    // "-"が頭についている引数は無視する
    args = args[1 .. $];
    while(args.length && args[0].startsWith("-"))
        args = args[1 .. $];

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
                    submitToQueue(args, cmdargs[1].to!uint);
                    break;
                default:
                    throw new Exception("Unsupported command: %s".format(cmdargs[0]));
            }
        }
    }
}



void submitToQueue(string[] args, uint len)
{
    foreach(i; iota(len).runAsTasks) {
        auto p = pipe();
        auto pid = spawnProcess(args, p.readEnd);
        p.writeEnd.writeln(i);
        p.writeEnd.flush();
        wait(pid);
    }
}

