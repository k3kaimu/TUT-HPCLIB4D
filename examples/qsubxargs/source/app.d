module app;

import std.algorithm;
import std.array;
import std.datetime;
import std.exception;
import std.file;
import std.format;
import std.getopt;
import std.path;
import std.process;
import std.stdio;
import std.string;

import tuthpc.taskqueue;


bool flagDryRun = false;        // ジョブを投入せずに，コマンドのリストを出力する
bool flagVerbose = false;       // 冗長な出力を含む


void main(string[] args)
{
    auto env = defaultJobEnvironment;
    env.isEnabledRenameExeFile = false;             // この実行ファイルをコピーしない
    env.isEnabledQueueOverflowProtection = false;   // ユーザーに全てを委ねる
    env.isEnabledUserCheckBeforePush = false;       // ユーザーに全てを委ねる

     getopt(args,
        std.getopt.config.passThrough,
        "th:dryrun", &flagDryRun,
        "th:verbose", &flagVerbose);

    if(args.length == 1) {
        printUsage();
        return;
    }

    // プログラム名を無視
    args = args[1 .. $];

    // "--th:"が頭についている引数は無視する
    while(args.length && args[0].startsWith("--th:"))
        args = args[1 .. $];

    if(args.length == 0) {
        printUsage();
        return;
    }


    // ログ出力用のディレクトリ名
    if(env.jobName) {
        env.logdir = format("logs_%s", env.jobName);
    } else if(env.scriptPath) {
        env.logdir = format("logs_%s", env.scriptPath.baseName.stripExtension);
    } else {
        env.logdir = format("logs_%s", Clock.currTime.toISOString());
    }

    env.logdir = env.saveOrLoadENV("TUTHPCLIB_QSUBARRAY_LOGDIR", env.logdir);


    // logdir/scripts 以下にシェルスクリプトを保存する
    if(thisProcessType() == ChildProcessType.SUBMITTER) {
        // コマンドのリストを作成
        string[] commands = makeCommandLines(args);
        if(flagDryRun) {
            writefln("%-(%s\n%)", commands);
            return;
        }

        immutable scriptDir = buildPath(env.logdir, "scripts");
        mkdirRecurse(scriptDir);

        string[] scriptList;
        foreach(index, cmd; commands) {
            string scriptPath = buildPath(scriptDir, "%d.sh".format(index));
            scriptList ~= scriptPath;
            File script = File(scriptPath, "w");
            script.write(cmd);
        }

        File scriptListFile = File(buildPath(env.logdir, "scriptlist.txt"), "w");
        scriptListFile.writef("%-(%s\n%)", scriptList);
    }


    // ジョブの作成
    File scriptListFile = File(buildPath(env.logdir, "scriptlist.txt"));
    auto taskList = new MultiTaskList!void();
    foreach(scriptPath; scriptListFile.byLine.map!chomp.map!dup) {
        taskList.append(&runScript, scriptPath);
    }

    // ジョブの投入 or 実行
    taskList.run(env);

    // foreach(i; 0 .. taskList.length) taskList[i]();
}


string[] makeCommandLines(const(string)[] xargsOptions)
{
    string[] program = ["xargs"];

    // "-"が頭についている引数を追加する
    while(xargsOptions.length && xargsOptions[0].startsWith("-")) {
        program ~= xargsOptions[0];
        xargsOptions = xargsOptions[1 .. $];
    }

    program ~= "echo";
    program ~= xargsOptions;    // 残りの引数を追加する

    auto stdoutPipe = pipe();
    auto xargsPid = spawnProcess(program, stdin, stdoutPipe.writeEnd, stderr);
    scope(failure) kill(xargsPid);
    scope(success) wait(xargsPid);

    // xargs ... echo ... の出力の各行を配列にする
    return stdoutPipe.readEnd.byLine.map!chomp.map!idup.array();
}


void runScript(const(char)[] filename)
{
    string cmd = readText(filename);
    if(flagVerbose) {
        writefln("file: %s", filename);
        writefln("command: %s", cmd);
    }

    auto cmdPid = spawnShell(cmd, std.stdio.stdin, std.stdio.stdout, std.stdio.stderr);
    scope(failure) kill(cmdPid);
    // scope(success) wait(cmdPid);

    int status = wait(cmdPid);

    if(flagVerbose) {
        writefln("exit status: %d", status);
    }
}


void printUsage()
{
    writeln(strUsage);
}


immutable string strUsage = `
Usage:
    qsubxargs <tuthpc-lib options...> <xargs options...> <commands...>

Where:
    <tuthpc-lib options...>:    options for qsubxargs
    <xargs options...>:         options for xargs
    <commands...>:              commands

For example:
    ls -1 | qsubxargs --th:g=28 --th:m=100 -l echo
`;