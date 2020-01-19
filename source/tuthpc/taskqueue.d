module tuthpc.taskqueue;

import core.runtime;

import tuthpc.mail;
import tuthpc.cluster;
public import tuthpc.tasklist;
import tuthpc.limiter;
import tuthpc.variable;

import std.algorithm;
import std.ascii;
import std.conv;
import std.datetime;
import std.digest.crc;
import std.digest.sha;
import std.exception;
import std.format;
import std.file;
import std.functional;
import std.path;
import std.process;
import std.range;
import std.stdio;
import std.string;
import std.traits;


enum EnvironmentKey : string
{
    RUN_ID = "TUTHPC_JOB_ENV_RUN_ID",
    ARRAY_ID = "TUTHPC_JOB_ENV_ARRAY_ID",
    TASK_ID = "TUTHPC_JOB_ENV_TASK_ID",
    PBS_JOBID = "PBS_JOBID",
    CLUSTER_NAME = "TUTHPC_CLUSTER_NAME",
    EMAIL_ADDR = "TUTHPC_EMAIL_ADDR",
    QSUB_ARGS = "TUTHPC_QSUB_ARGS",
    EXPORT_ENVS = "TUTHPC_EXPORT_ENVS",
    DEFAULT_ARGS = "TUTHPC_DEFAULT_ARGS",
    STARTUP_SCRIPT = "TUTHPC_STARTUP_SCRIPT",
}


enum ChildProcessType : string
{
    SUBMITTER = "TUTHPC_SUBMITTER",
    ANALYZER = "TUTHPC_ANALYZER",
    TASK_MANAGER = "TUTHPC_TASK_MANAGER",
    TASK_PROCESSOR = "TUTHPC_TASK_PROCESSOR",
}


ChildProcessType thisProcessType()
{
    auto arg = Runtime.args.join(" ");

    foreach(type; EnumMembers!ChildProcessType) {
        if(arg.canFind(cast(string)type))
            return type;
    }

    return ChildProcessType.SUBMITTER;
}


string[] filteredRuntimeArgs()
{
    static string[] dst;

    if(dst is null) {
        LnextArg:
        foreach(e; Runtime.args[1 .. $]) {
            foreach(type; EnumMembers!ChildProcessType)
                if(e == cast(string)type)
                    continue LnextArg;

            dst ~= e;
        }
    }

    return dst;
}


string hashOfExe()
{
    import std.file;

    static string result;
    if(result is null) {
        result = crc32Of(cast(ubyte[])std.file.read(Runtime.args[0])).toHexString.dup;
    }

    return result;
}


enum DependencySetting
{
    success = "afterokarray",
    failure = "afternotokarray",
    exit = "afteranyarray",
    success_single = "afterok",
    failure_single = "afternotok",
    exit_single = "afterany",
}


class JobEnvironment
{
    bool useArrayJob = true;    /// ArrayJobにするかどうか
    string scriptPath;          /// スクリプトファイルの保存場所, nullならパイプでqsubにジョブを送る
    string queueName;           /// nullのとき，自動でclusters[cluster].queueNameに設定される
    string jobName;             /// ジョブの名前
    string dependentJob;        /// 依存しているジョブのID
    string dependencySetting = DependencySetting.exit;   /// 依存しているジョブがどの状況でこのジョブを実行するか
    string[] unloadModules;     /// module unloadで破棄されるモジュール
    string[] loadModules;       /// module loadで読み込まれるモジュール
    string[string] envs;        /// 環境変数
    bool isEnabledRenameExeFile = true; /// 実行ジョブファイル名を，バイナリ表現にもとづきユニークな名前に変更します．
    string originalExePath;     /// リネーム・コピー前の実行ファイルのパス
    string renamedExePath;      /// リネーム・コピー後の実行ファイルのパス
    string[] prescript;         /// プログラム実行前に実行されるシェルスクリプト
    string[] jobScript;         /// ジョブスクリプト
    string[] postscript;        /// プログラム実行後に実行されるシェルスクリプト
    uint taskGroupSize = 0;  /// nodes=1:ppn=1のとき，ppn=<taskGroupSize>で投入し，並列実行します
    uint ppn = 1;               /// 各ノードで何個のプロセッサを使用するか
    uint nodes = 1;             /// 1つのジョブあたりで実行するノード数
    int mem = -1;               /// 0なら自動設定
    int pmem = -1;              /// 0なら自動設定
    int vmem = -1;              /// 0なら自動設定
    int pvmem = -1;             /// 0なら自動設定
    Duration walltime;          /// walltime, 0なら自動で最大値に設定される
    bool isEnabledEmailOnError = true;  /// エラー時にメールで通知するかどうか
    bool isEnabledEmailOnStart = false; /// 実行開始時にメールで通知するかどうか
    bool isEnabledEmailOnEnd = false;   /// 実行終了時にメールで通知するかどうか
    string[] emailAddrs;                /// メールを送りたい宛先
    uint maxArraySize = 8192;           /// アレイジョブでの最大のサイズ
    uint maxSlotSize = 0;               /// アレイジョブでの最大スロット数(-t 1-100%5の5のこと), 0は無設定（無制限）
    bool isEnabledQueueOverflowProtection = true;   /// キューの最大値(4096)以上ジョブを投入しないようにする
    bool isEnabledUserCheckBeforePush = true;       /// ジョブを投げる前にユーザーに確認を要求するかどうか
    bool isForcedCommandLineArgs = true;            /// コマンドライン引数による指定で実行時の値を上書きするか
    //bool isEnabledSpawnNewProcess = true;   /// 各タスクは新しいプロセスを起動する
    size_t totalProcessNum = 0;
    string logdir;              /// 各タスクの標準出力，標準エラーを保存するディレクトリを指定できる
    bool isShowMode = false;    /// 各タスクの実行結果を表示するためのモード



    void applyCommandLineArgs(in string[] args)
    {
        import std.getopt;
        int walltime_int = -1;

        auto newargs = args.dup;
        getopt(newargs,
            std.getopt.config.passThrough,
            "th:queue|th:q", &queueName,
            "th:name", &jobName,
            "th:after|th:a", &dependentJob,
            "th:ppn|th:p", &ppn,
            "th:nodes|th:n", &nodes,
            "th:mem", &mem,
            "th:pmem|th:pm", &pmem,
            "th:taskGroupSize|th:g", &taskGroupSize,
            "th:walltime|th:w", &walltime_int,
            "th:mailOnError|th:me", &isEnabledEmailOnError,
            "th:mailOnStart|th:ms", &isEnabledEmailOnStart,
            "th:mailOnFinish|th:mf", &isEnabledEmailOnEnd,
            "th:mailTo", &emailAddrs,
            "th:maxArraySize|th:m", &maxArraySize,
            "th:maxSlotSize|th:s", &maxSlotSize,
            "th:queueOverflowProtection|th:qop", &isEnabledQueueOverflowProtection,
            "th:requireUserCheck", &isEnabledUserCheckBeforePush,
            "th:maxProcessNum|th:plim", &totalProcessNum,
            "th:forceCommandLineArgs", &isForcedCommandLineArgs,
            "th:logdir", &logdir,
            "th:scriptLog", &scriptPath,
            "th:show", &isShowMode
        );

        if(walltime_int != -1)
            walltime = walltime_int.hours;
    }


    void autoSetting()
    {
        auto cluster = ClusterInfo.currInstance;

        if(isForcedCommandLineArgs) {
            if(EnvironmentKey.DEFAULT_ARGS in environment) this.applyCommandLineArgs(Runtime.args[0] ~ environment[EnvironmentKey.DEFAULT_ARGS].split(","));
            this.applyCommandLineArgs(Runtime.args);
        }

        if(cluster !is null && queueName is null) queueName = cluster.defaultQueueName;
        if(ppn == 0) ppn = 1;
        if(nodes == 0) nodes = 1;

        if(cluster !is null && nodes == 1 && ppn == 1 && taskGroupSize == 0) {
            if(cast(TUTWInfo)cluster) { // TUTクラスタかそうでないか
                taskGroupSize = 11;
            } else {
                taskGroupSize = cluster.maxPPN;
            }
        }else if(cluster is null || nodes != 1 || ppn != 1){
            taskGroupSize = 1;
        }

        if(cluster !is null) {
            int maxMem = (cluster.maxMemGB * 1000 / cluster.maxPPN) * (ppn * taskGroupSize) / 1000,
                maxMemPerPS = maxMem / (ppn * taskGroupSize);

            if(mem == 0) mem = maxMem;
            if(pmem == 0) pmem = maxMemPerPS;
            if(vmem == 0) vmem = maxMem;
            if(pvmem == 0) pvmem = maxMemPerPS;
        }

        if(mem <= 0 && pmem > 0) {
            mem = pmem * ppn * taskGroupSize;
        } else if(mem > 0 && pmem <= 0) {
            pmem = mem / (ppn * taskGroupSize);
        }

        if(vmem <= 0 && mem > 0) vmem = mem;
        if(pvmem <= 0 && pmem > 0) pvmem = pmem;

        if(walltime == 0.seconds){
            switch(queueName){
                case "wLrchq":
                    walltime = 335.hours;
                    break;
                case "gr10061b":
                    walltime = 335.hours;
                    break;
                default:
                    walltime = 1.hours;
            }
        }

        if(isEnabledEmailOnStart || isEnabledEmailOnEnd || isEnabledEmailOnError) {
            if(emailAddrs.length == 0 && EnvironmentKey.EMAIL_ADDR in environment) {
                emailAddrs = [environment[EnvironmentKey.EMAIL_ADDR]];
            }
        }

        if(jobScript is null){
            originalExePath = Runtime.args[0];
            if(isEnabledRenameExeFile && renamedExePath.walkLength == 0) {
                renamedExePath = format("./%s_%s",
                    originalExePath.baseName,
                    crc32Of(cast(ubyte[])std.file.read(originalExePath)).toHexString);
            } else {
                renamedExePath = originalExePath;
            }

            jobScript = [format("%s %-('%s'%| %) %s", renamedExePath, filteredRuntimeArgs(), cast(string)ChildProcessType.TASK_MANAGER)];

            if(isEnabledEmailOnError && emailAddrs.length != 0){
                if(environment.get("MAILGUN_APIKEY")
                && environment.get("MAILGUN_DOMAIN"))
                {
                    jobScript[0] = format("(%s) || (%s)", jobScript[0],
                            (   `curl -s --user 'api:%s'`
                                ~ ` https://api.mailgun.net/v3/%s/messages`
                                ~ ` -F from='TUTHPCLib <mailgun@%s>'`
                                ~ ` -F to='%s'`
                                ~ ` -F subject='%s'`
                                ~ ` -F text="%s"`
                            ).format(
                                environment.get("MAILGUN_APIKEY"),
                                environment.get("MAILGUN_DOMAIN"),
                                environment.get("MAILGUN_DOMAIN"),
                                emailAddrs.join(','),
                                "FATAL ERROR: Maybe segmentation fault?",
                                "$(env)")
                        );
                }
            }

            jobScript ~= "echo $?";
        }

        if(cluster !is null && totalProcessNum == 0) {
            totalProcessNum = cluster.maxPPN * 5;
        }
    }


    void copyExeFile()
    {
        if(!isEnabledRenameExeFile)
            return;

        // check that the renamed file already exists
        if(exists(renamedExePath))
            return;

        writefln("copy: %s -> %s", originalExePath, renamedExePath);
        std.file.copy(originalExePath, renamedExePath);
        enforce(execute(["chmod", "+x", renamedExePath]).status == 0);
    }


    JobEnvironment dup() const
    {
        JobEnvironment newval = new JobEnvironment();

        foreach(i, ref e; this.tupleof){
            alias E = Unqual!(typeof(e));

            static if(is(E : ulong) || is(E : string) || is(E : Duration)){
                newval.tupleof[i] = e;
            }else static if(isArray!(E)) {
                foreach(v; e)
                    newval.tupleof[i] ~= v;
            }else static if(is(E : const(string)[string])){
                newval.tupleof[i] = null;
                foreach(k, v; e)
                    newval.tupleof[i][k] = v;
            }else
                static assert(0, E.stringof);
        }

        return newval;
    }
}


JobEnvironment defaultJobEnvironment(string[] args = Runtime.args)
{
    JobEnvironment env = new JobEnvironment();
    if(EnvironmentKey.DEFAULT_ARGS in environment) env.applyCommandLineArgs(args[0] ~ environment[EnvironmentKey.DEFAULT_ARGS].split(","));
    env.applyCommandLineArgs(args);
    return env;
}


void makeQueueScript(R)(ref R orange, ClusterInfo cluster, in JobEnvironment jenv, size_t jobCount = 1)
{
    string headerID = "PBS";
    if(cast(KyotoBInfo)cluster)
        headerID = "QSUB";

    .put(orange, "#!/bin/bash\n");

    // resource(nodes, ppn,)
    if(auto cinfo = cast(TUTWInfo)cluster) {
        orange.formattedWrite("#PBS -l nodes=%s:ppn=%s", jenv.nodes, jenv.ppn * jenv.taskGroupSize);
        if(jenv.mem != -1) orange.formattedWrite(",mem=%sgb", jenv.mem);
        if(jenv.pmem != -1) orange.formattedWrite(",pmem=%sgb", jenv.pmem);
        if(jenv.vmem != -1) orange.formattedWrite(",vmem=%sgb", jenv.vmem);
        if(jenv.pvmem != -1) orange.formattedWrite(",pvmem=%sgb", jenv.pvmem);
    } else if(auto kyotobInfo = cast(KyotoBInfo)cluster) {
        auto reqcpus = jenv.ppn * jenv.taskGroupSize;
        if(kyotobInfo.enableHTT){
            orange.formattedWrite("#QSUB -A p=%s:t=%s:c=%s", jenv.nodes, (reqcpus + 1) / 2 * 2, (reqcpus + 1) / 2);
        }else
            orange.formattedWrite("#QSUB -A p=%s:t=%s:c=%s", jenv.nodes, reqcpus, reqcpus);

        if(jenv.pmem != -1) orange.formattedWrite(":m=%sG", jenv.pmem);
    } else {
        orange.formattedWrite("#PBS -l select=%s:ncpus=%s", jenv.nodes, jenv.ppn * jenv.taskGroupSize);
        if(jenv.mem != -1) orange.formattedWrite(":mem=%sgb", jenv.mem);
    }

    .put(orange, '\n');

    if(jenv.jobName !is null) {
        orange.formattedWrite("#%s -N %s\n", headerID, jenv.jobName);
    }

    // walltime
    if(headerID == "PBS") {
        int hrs, mins, secs;
        jenv.walltime.split!("hours", "minutes", "seconds")(hrs, mins, secs);
        orange.formattedWrite("#PBS -l walltime=%d:%02d:%02d\n", hrs, mins, secs);
    } else if(headerID == "QSUB") {
        int hrs, mins, secs;
        jenv.walltime.split!("hours", "minutes", "seconds")(hrs, mins, secs);
        orange.formattedWrite("#QSUB -W %d:%d\n", hrs, mins);
    } else {
        enforce(0, "headerID == '%s' and it is unknown value.".format(headerID));
    }

    orange.formattedWrite("#%s -q %s\n", headerID, jenv.queueName);

    if(cast(KyotoBInfo)cluster !is null) {
        orange.formattedWrite("#%s -ug gr10061\n", headerID);
    }

    // array job
    if(jenv.useArrayJob) {
        if(auto cinfo = cast(TUTWInfo)cluster) {
            if(jobCount > 1) {
                orange.formattedWrite("#PBS -t %s-%s\n", 0, jobCount-1);

                if(jenv.maxSlotSize != 0) {
                    orange.put('%');
                    orange.formattedWrite("%s", jenv.maxSlotSize);
                }
            }
        } else if(auto cinfo = cast(KyotoBInfo)cluster) {
            if(jobCount > 1) {
                orange.formattedWrite("#QSUB -J %s-%s\n", 0, jobCount-1);
                if(jenv.maxSlotSize != 0)
                    writeln("In this cluster, maxSlotSize is ignored.");
            }
        }
        else {
            if(jobCount > 1) {
                orange.formattedWrite("#PBS -J %s-%s\n", 0, jobCount-1);
                if(jenv.maxSlotSize != 0)
                    writeln("In this cluster, maxSlotSize is ignored.");
            }
        }
    }

    if((jenv.isEnabledEmailOnStart || jenv.isEnabledEmailOnEnd || jenv.isEnabledEmailOnError) && jenv.emailAddrs.length != 0) {
        orange.formattedWrite("#%s -m ", headerID);
        if(jenv.isEnabledEmailOnStart)  .put(orange, 'b');
        if(jenv.isEnabledEmailOnEnd)    .put(orange, 'e');
        if(jenv.isEnabledEmailOnError)  .put(orange, 'a');
        .put(orange, '\n');
        orange.formattedWrite("#%s -M %-(%s %)\n", headerID, jenv.emailAddrs);
    }

    if(EnvironmentKey.QSUB_ARGS in environment) {
        orange.formattedWrite("#%s %s\n", headerID, environment[EnvironmentKey.QSUB_ARGS]);
    }

    //.put(orange, "set -e\n");

    // 引き継ぐ環境変数の設定
    if(EnvironmentKey.EXPORT_ENVS in environment) {
        foreach(k; environment[EnvironmentKey.EXPORT_ENVS].split(",")) {
            if(auto v = environment.get(k, null))
                orange.formattedWrite("export %s='%s'\n", k, v);
        }
    }

    // EnvironmentKeyにあるもののうち，現在設定されているものを引き継ぐ
    foreach(k; EnumMembers!EnvironmentKey) {
        if(auto v = environment.get(k, null))
            orange.formattedWrite("export %s='%s'\n", cast(string)k, v);
    }

    // 起動時に走らせるスクリプト
    if(EnvironmentKey.STARTUP_SCRIPT in environment) {
        orange.formattedWrite("%s\n", environment[EnvironmentKey.EXPORT_ENVS]);
    } else {
        orange.formattedWrite("%s\n", "if [ -f ~/.bashrc ] ; then source ~/.bashrc; fi");
    }

    .put(orange, "MPI_PROCS=`wc -l $PBS_NODEFILE | awk '{print $1}'`\n");

    if(jenv.useArrayJob && jobCount == 1) {
        orange.formattedWrite("export %s=0\n", cluster.arrayIDEnvKey);
    }

    orange.formattedWrite("export %s=${%s}\n", cast(string)EnvironmentKey.ARRAY_ID, cluster.arrayIDEnvKey);

    if(headerID == "PBS")
        .put(orange, "cd $PBS_O_WORKDIR\n");
    else if(headerID == "QSUB")
        .put(orange, "cd $QSUB_WORKDIR\n");
    else
        enforce(0, "headerID == '%s' and it is unknown value.".format(headerID));

    foreach(e; jenv.unloadModules) orange.formattedWrite("module unload %s\n", e);
    foreach(e; jenv.loadModules) orange.formattedWrite("module load %s\n", e);
    foreach(k, v; jenv.envs) orange.formattedWrite("export %s=%s\n", k, v);
    foreach(line; jenv.prescript) { .put(orange, line); .put(orange, '\n'); }
    foreach(line; jenv.jobScript) { .put(orange, line); .put(orange, '\n'); }
    foreach(line; jenv.postscript) { .put(orange, line); .put(orange, '\n'); }
}


struct RunState
{
  static:
    size_t countOfCallRun;
    bool nowInRun;
}


struct PushResult(T)
{
    Flag!"isAborted" isAborted;     // ユーザーによりジョブを投げるのが中止された
    string jobId;

    static if(!is(T == void))
    {
        OnDiskVariable!T[] retvals;
    }
}


struct QueueOverflowProtector
{
    static int countOfEnqueuedJobs = -1;
    static int countOfTotalMyJobs = 0;
    static bool alreadySpawnAnalyzer = false;


    static
    bool isAnalyzerProcess()
    {
        return thisProcessType() == ChildProcessType.ANALYZER;
    }


    static
    void analyze(size_t jobSize, size_t runId, string file, size_t line)
    {
        auto cinfo = ClusterInfo.currInstance;
        enforce(thisProcessType() == ChildProcessType.SUBMITTER
            ||  thisProcessType() == ChildProcessType.ANALYZER);

        if(isAnalyzerProcess){
            countOfTotalMyJobs += jobSize;
            writefln("ANALYZE: %s jobs are spawned on %s(%s)[%s]", jobSize, file, line, runId);
            enforce(countOfTotalMyJobs < 8000,
                "Your jobs may cause queue overflow. Please use env.maxArraySize.");
        }else{
            if(!alreadySpawnAnalyzer){
                string cmd = format("%s %-('%s'%| %) %s", Runtime.args[0], filteredRuntimeArgs(), cast(string)ChildProcessType.ANALYZER);
                writefln("The job analyzer is spawned with \"%s\".", cmd);

                auto analyzer = executeShell(cmd);
                enforce(analyzer.status == 0, "Job analyer is failed. Output of the analyzer is following:\n" ~ analyzer.output);
                writeln(analyzer.output);
                alreadySpawnAnalyzer = true;
            }
        }
    }
}


string logDirName(in JobEnvironment env, size_t runId)
{
    if(env.logdir.length != 0)
        return env.logdir;
    else
        return format("logs_%s_%s", hashOfExe(), runId);
}


string makeOnDiskVariableName(size_t taskIndex)
{
    return "retval_%s.bin".format(taskIndex);
}


Pid spawnTask(in string[] args, JobEnvironment jenv, size_t taskIndex, string logdir, string file, size_t line, string[string] addEnvs = null)
{
    import std.path;

    if(ClusterInfo.currInstance)
    {
        enforce(numOfProcessOfUser() <= jenv.totalProcessNum,
            "Many processes are spawned.\n Process List:\n%(%s\n%)".format(pgrepByUser()));
    }


    string outname = buildPath(logdir, format("stdout_%s.log", taskIndex));
    string errname = buildPath(logdir, format("stderr_%s.log", taskIndex));

    string[string] env = [EnvironmentKey.TASK_ID : taskIndex.to!string];
    foreach(k, v; addEnvs) env[k] = v;

    auto pid = spawnProcess(args ~ (cast(string) ChildProcessType.TASK_PROCESSOR), std.stdio.stdin, File(outname, "w"), File(errname, "w"), env);
    return pid;
}


void copyLogTextToStdOE(int status, Duration time, JobEnvironment jenv, size_t taskIndex, string logdir, string file, size_t line)
{
    import std.path;
    import std.stdio;
    import std.file;

    string outname = buildPath(logdir, format("stdout_%s.log", taskIndex));
    string errname = buildPath(logdir, format("stderr_%s.log", taskIndex));

    {
        auto writer = stdout.lockingTextWriter;
        writer.formattedWrite("=========== START OF %sTH TASK. ===========\n", taskIndex);
        std.stdio.write(readText(outname));
        writer.formattedWrite("=========== END OF %sTH TASK. (status = %s, time = %s [s]) ===========\n", taskIndex, status, time.total!"seconds");
    }

    {
        File log = File(errname, "r");
        auto writer = stderr.lockingTextWriter;
        bool bFirst = true;
        foreach(str; log.byLine){
            if(bFirst){
                writer.formattedWrite("=========== START OF %sTH TASK. ===========\n", taskIndex);
                bFirst = false;
            }
            writer.put(str);
            writer.put('\n');
        }

        if(!bFirst){
            writer.formattedWrite("=========== END OF %sTH TASK. (status = %s) ===========\n", taskIndex, status);
        }
    }

    if(status != 0 && jenv.isEnabledEmailOnError && jenv.emailAddrs.length != 0){
        import std.socket;
        string[2][] info;
        auto jobid = environment.get(EnvironmentKey.PBS_JOBID, "Unknown");
        info ~= ["PBS_JOBID",           jobid];
        info ~= ["FileName",            file];
        info ~= ["Line",                line.to!string];
        if(auto s = environment.get(EnvironmentKey.RUN_ID))   info ~= [EnvironmentKey.RUN_ID,       s];
        if(auto s = environment.get(EnvironmentKey.ARRAY_ID)) info ~= [EnvironmentKey.ARRAY_ID,     s];
        if(auto s = environment.get(EnvironmentKey.TASK_ID))  info ~= [EnvironmentKey.TASK_ID,      s];
        info ~= ["TUTHPC_JOB_ENV_TASK_ID",      taskIndex.to!string];
        info ~= ["PBS_ARRAYID",         environment.get("PBS_ARRAYID", "Unknown")];
        info ~= ["PBS_ARRAY_INDEX",         environment.get("PBS_ARRAY_INDEX", "Unknown")];
        //info ~= ["Job size",            taskList.length.to!string];
        //info ~= ["Start time",          startTime.toISOExtString()];
        info ~= ["End time",            Clock.currTime.toISOExtString()];
        info ~= ["Host",                Socket.hostName()];
        info ~= ["Process",             format("%-(%s %)", Runtime.args)];
        info ~= ["stdout",              readText(outname)];
        info ~= ["stderr",              readText(errname)];

        tuthpc.mail.sendMail(
            jenv.emailAddrs.join(','),
            "Error on Job %s".format(jobid),
            "%(%-(%s: \t%)\n%)".format(info)
        );
    }
}


void processTasks(R, TL)(in string[] args, JobEnvironment jenv, uint parallelSize, size_t runId, R taskIndxs, TL taskList, string logdir, string file, size_t line, string[string] addEnvs = null)
{
    import std.path;
    import std.process;
    import std.file;

    enforce(thisProcessType() == ChildProcessType.TASK_MANAGER
        ||  thisProcessType() == ChildProcessType.TASK_PROCESSOR);

    if(auto strOfTaskID = environment.get(EnvironmentKey.TASK_ID)){
        immutable size_t taskIndex = strOfTaskID.to!size_t();

        enforce(thisProcessType() == ChildProcessType.TASK_PROCESSOR);
        enforce(environment.get(EnvironmentKey.RUN_ID, "-1").to!long == RunState.countOfCallRun);

        import core.thread;
        try {   // 環境によっては失敗する可能性がある（WSL上のUbuntuでは例外送出）
            Thread.getThis.priority = Thread.PRIORITY_MIN;
        } catch(core.thread.ThreadException ex){
            //stderr.writeln(ex);
        }

        alias ReturnType = typeof(taskList[taskIndex]());

        static if(is(ReturnType == void))
            taskList[taskIndex]();
        else { 
            auto var = OnDiskVariable!ReturnType(buildPath(logdir, makeOnDiskVariableName(taskIndex)));
            var = taskList[taskIndex]();
        }
    }else{
        enforce(thisProcessType() == ChildProcessType.TASK_MANAGER);
        enforce(exists(logdir));

        static struct ProcessState
        {
            Pid pid;
            size_t taskIndex;
            SysTime startTime;
        }

        ProcessState*[] procList = new ProcessState*[](parallelSize);
        scope(failure){
            procList.filter!"a".each!(a => std.process.kill(a.pid));
        }

        while(!(taskIndxs.empty && procList.all!"a is null"))
        {
            foreach(ref proc; procList) {
                if(proc is null && !taskIndxs.empty) {
                    immutable size_t taskIndex = taskIndxs.front;
                    taskIndxs.popFront();
                    proc = new ProcessState(
                        spawnTask(args, jenv, taskIndex, logdir, file, line, addEnvs),
                        taskIndex,
                        Clock.currTime);
                }

                if(proc !is null) {
                    auto status = tryWait(proc.pid);
                    if(status.terminated) {
                        auto procTime = Clock.currTime() - proc.startTime;  // 処理にかかった時間を計算
                        copyLogTextToStdOE(status.status, procTime, jenv, proc.taskIndex, logdir, file, line);
                        proc = null;
                    }
                }
            }

            import core.thread;
            Thread.sleep(1.seconds);
        }
    }
}


/**
ジョブを投入するために起動されたプロセスでのみデリゲートdg()を実行する
*/
void runOnlyMainProcess(void delegate() dg)
{
    if(thisProcessType() == ChildProcessType.SUBMITTER) {
        dg();
    }
}


PushResult!(ReturnTypeOfTaskList!TL) run(TL)(TL taskList, in JobEnvironment envIn = defaultJobEnvironment(), string file = __FILE__, size_t line = __LINE__)
if(isTaskList!TL)
{
    alias ReturnType = ReturnTypeOfTaskList!TL;

    auto env = envIn.dup;
    auto cluster = ClusterInfo.currInstance;
    env.useArrayJob = true;

    if(cluster !is null && env.nodes == 1 && env.ppn == 1 && env.taskGroupSize == 0){
        if(taskList.length <= cluster.maxPPN * 2)
            env.taskGroupSize = 1;
        else if(taskList.length < 220)
            env.taskGroupSize = 7;
        else
            env.taskGroupSize = 11;
    }

    env.autoSetting();

    PushResult!ReturnType result;
    immutable nowInRunOld = RunState.nowInRun;

    RunState.nowInRun = true;
    scope(exit) {
        RunState.nowInRun = nowInRunOld;
        if(!RunState.nowInRun)
            ++RunState.countOfCallRun;
    }

    size_t arrayJobSize = taskList.length / env.taskGroupSize + (taskList.length % env.taskGroupSize != 0 ? 1 : 0);
    if(arrayJobSize > env.maxArraySize)
        arrayJobSize = env.maxArraySize;

    immutable logdir = logDirName(env, RunState.countOfCallRun);

    if(env.isShowMode)
        goto Lreturn;

    if(thisProcessType() == ChildProcessType.SUBMITTER || thisProcessType() == ChildProcessType.ANALYZER)
    {
        enforce(nowInRunOld == false);
        enforce(env.useArrayJob);

        if(env.isEnabledQueueOverflowProtection)
        {
            QueueOverflowProtector.analyze(arrayJobSize, RunState.countOfCallRun, file, line);

            if(QueueOverflowProtector.isAnalyzerProcess)
                goto Lreturn;
        }

        enforce(thisProcessType() == ChildProcessType.SUBMITTER);

        // ジョブを投げる前に投げてよいかユーザーに確かめる
        if(env.isEnabledUserCheckBeforePush) {
            writeln("A new array job will be submitted:");
            writefln("\ttaskList.length: %s", taskList.length);
            writefln("\tArray Job Size: %s", arrayJobSize);
            writefln("\tLog directory: %s", logdir);
            write("Do you submit this job? [y|N] --- ");

            auto userInput = readln().chomp;
            if(userInput != "y" && userInput != "Y"){
                writeln("This submission is aborted by the user.\n");
                return typeof(return)(Yes.isAborted, "");
            }
        }

        import std.file : mkdirRecurse, exists;
        if(!exists(logdir))
            mkdirRecurse(logdir);

        // 実行ファイルのコピー
        env.copyExeFile();

        // ジョブを投げる
        result = pushArrayJobToQueue!(ReturnType)(
            RunState.countOfCallRun.to!string,
            arrayJobSize, env, cluster,
            file, line);

        writeln("ID: ", result.jobId);
        writefln("\ttaskList.length: %s", taskList.length);
        writefln("\tArray Job Size: %s", arrayJobSize);
        writefln("\tLog directory: %s", logdir);
        writeln();
    }
    else if(cluster !is null && nowInRunOld == false 
        && (thisProcessType() == ChildProcessType.TASK_MANAGER || thisProcessType() == ChildProcessType.TASK_PROCESSOR) )
    {
        enforce(
                environment.get(EnvironmentKey.RUN_ID)
             && environment.get(EnvironmentKey.ARRAY_ID), "cannot find environment variables: 'TUTHPC_JOB_ENV_RUN_ID', and 'TUTHPC_JOB_ENV_ARRAY_ID'");

        if(environment[EnvironmentKey.RUN_ID].to!size_t == RunState.countOfCallRun)
        {
            size_t index = environment[EnvironmentKey.ARRAY_ID].to!size_t();
            enforce(index < arrayJobSize);

            index *= env.taskGroupSize;

            size_t[] taskIndexList;
            foreach(p; 0 .. env.taskGroupSize)
                for(size_t taskIndex = index + p; taskIndex < taskList.length; taskIndex += env.maxArraySize * env.taskGroupSize)
                    taskIndexList ~= taskIndex;

            processTasks(Runtime.args[0] ~ filteredRuntimeArgs(), env, env.taskGroupSize, RunState.countOfCallRun, taskIndexList, taskList, logdir, file, line);
        }
    }
    else if(nowInRunOld == true)
    {
        static if(is(ReturnType == void))
        {
            foreach(i; 0 .. taskList.length)
                taskList[i]();
        }
        else
        {
            foreach(i; 0 .. taskList.length) {
                auto var = OnDiskVariable!ReturnType(null, Yes.isReadOnly);
                var = taskList[i]();
                result.retvals ~= var;
            }
        }
    }
    else
    {
        import std.parallelism;

        // Task実行プロセスかどうかチェック
        if(EnvironmentKey.RUN_ID in environment){
            // 別のrunのために起動されたプロセスなら即時終了
            if(environment[EnvironmentKey.RUN_ID].to!size_t != RunState.countOfCallRun) goto Lreturn;
        }else{
            // Task実行プロセスで必要なため，管理するプロセスでは予めログ用のディレクトリを作っておく
            import std.file : mkdir;
            mkdir(logdir);

            // 実行ファイルのコピー
            env.copyExeFile();
        }

        uint parallelSize = std.parallelism.totalCPUs / env.ppn;
        env.envs[EnvironmentKey.RUN_ID] = RunState.countOfCallRun.to!string;
        processTasks(env.renamedExePath ~ filteredRuntimeArgs(), env, parallelSize, RunState.countOfCallRun, iota(taskList.length), taskList, logdir, file, line, env.envs);
    }

  Lreturn:
    static if(!is(ReturnType == void))
    {
        if(taskList.length != 0 && result.retvals.length == 0) {
            foreach(i; 0 .. taskList.length)
                result.retvals ~= OnDiskVariable!ReturnType(buildPath(logdir, makeOnDiskVariableName(i)), Yes.isReadOnly);
        }
    }

    return result;
}


private
PushResult!T pushArrayJobToQueue(T)(string runId, size_t arrayJobSize, JobEnvironment env, ClusterInfo cluster, string file, size_t line)
{
    env.envs[EnvironmentKey.RUN_ID] = runId;

    string dstJobId;
    string[] qsubcommands = ["qsub"];
    if(env.dependentJob.length != 0) {
        qsubcommands ~= "-W";
        qsubcommands ~= format("depend=%s:%s", cast(string)env.dependencySetting, env.dependentJob);
    }

    // if(EnvironmentKey.QSUB_ARGS in environment) {
    //     qsubcommands ~= environment[EnvironmentKey.QSUB_ARGS];
    // }

    if(env.scriptPath !is null){
        auto app = appender!string;

        makeQueueScript(app, cluster, env, arrayJobSize);

        import std.file;
        std.file.write(env.scriptPath, app.data);

        qsubcommands ~= env.scriptPath;
        auto qsub = execute(qsubcommands);
        dstJobId = qsub.output.until!(a => a != '.').array().to!string;
    }else{
        auto pipes = pipeProcess(qsubcommands, Redirect.stdin | Redirect.stdout);
        scope(exit) wait(pipes.pid);
        scope(failure) kill(pipes.pid);

        {
            auto writer = pipes.stdin.lockingTextWriter;
            makeQueueScript(writer, cluster, env, arrayJobSize);
        }
        pipes.stdin.flush();
        pipes.stdin.close();

        dstJobId = pipes.stdout.byLine.front.split('.')[0].array().to!string;
    }

    return typeof(return)(No.isAborted, dstJobId);
}


template afterRunImpl(DependencySetting ds)
{
    PushResult!(ReturnTypeOfTaskList!TL) afterRunImpl(X, TL)(PushResult!X parentJob, TL taskList, in JobEnvironment env = defaultJobEnvironment(), string file = __FILE__, size_t line = __LINE__)
    if(isTaskList!TL)
    {
        auto newenv = env.dup;

        if(parentJob.isAborted) {
            writeln("%s(%s): This job is aborted because the parent job of this job is aborted.", file, line);
            return typeof(return)(Yes.isAborted, "");
        }

        newenv.dependentJob = parentJob.jobId;
        newenv.dependencySetting = ds;

        auto cluster = ClusterInfo.currInstance;
        if(auto kyotobInfo = cast(KyotoBInfo)cluster) {
            newenv.autoSetting();
            runOnlyMainProcess({
                newenv.dependentJob = submitMonitoringJob(parentJob.jobId, newenv.queueName, cluster);
                newenv.dependencySetting = DependencySetting.success_single;
            });
        }

        return run(taskList, newenv, file, line);
    }
}


alias afterSuccessRun = afterRunImpl!(DependencySetting.success);
alias afterFailureRun = afterRunImpl!(DependencySetting.failure);
alias afterExitRun = afterRunImpl!(DependencySetting.exit);


template toTasks(alias fn)
{
    auto toTasks(R)(R range)
    if(isInputRange!R)
    {
        static if(hasLength!R && isRandomAccessRange!R)
            return range.map!(a => { return fn(a); });
        else
            return range.array().map!(a => { return fn(a); });
    }
}


unittest
{
    int a = -1;
    auto tasks = iota(10).toTasks!(b => a = b);
    static assert(isTaskList!(typeof(tasks)));

    assert(tasks.length == 10);

    foreach(i; 0 .. tasks.length){
        tasks[i]();
        assert(a == i);
    }
}


auto runAsTasks(R)(R range, in JobEnvironment env = defaultJobEnvironment(), string file = __FILE__, size_t line = __LINE__)
if(isInputRange!R)
{
    static struct RunAsTasksResult
    {
        alias E = ElementType!R;

        int opApply(int delegate(ref E) dg)
        {
            //int result = 0;
            auto taskList = new MultiTaskList!void();
            for(size_t i = 0; !_range.empty;){
                taskList.append((E e){ dg(e); }, _range.front);
                ++i;
                _range.popFront;
            }

            .run(taskList, _env, _filename, _line);

            return 0;
        }


        int opApply(int delegate(ref size_t, ref E) dg)
        {
            auto taskList = new MultiTaskList!void();
            for(size_t i = 0; !_range.empty;){
                taskList.append((size_t i, E e){ dg(i, e); }, i, _range.front);
                ++i;
                _range.popFront;
            }

            .run(taskList, _env, _filename, _line);
            return 0;
        }

      private:
        R _range;
        JobEnvironment _env;
        string _filename;
        size_t _line;
    }


    return RunAsTasksResult(range, env.dup, file, line);
}


auto appendAsTasks(R)(R range, MultiTaskList!void taskList)
if(isInputRange!R)
{
    static struct AppendAsTasksResult
    {
        alias E = ElementType!R;

        int opApply(int delegate(ref E) dg)
        {
            for(size_t i = 0; !_range.empty;){
                _taskList.append((E e){ dg(e); }, _range.front);
                ++i;
                _range.popFront;
            }

            return 0;
        }


        int opApply(int delegate(ref size_t, ref E) dg)
        {
            for(size_t i = 0; !_range.empty;){
                _taskList.append((size_t i, E e) { dg(i, e); }, i, _range.front);
                ++i;
                _range.popFront;
            }

            return 0;
        }

      private:
        R _range;
        MultiTaskList!void _taskList;
    }


    return AppendAsTasksResult(range, taskList);
}


/**
開発ノードで実行されたときは環境変数をジョブスクリプトに埋め込み，
計算ノードで実行されたときは環境変数を読み込みます
*/
string saveOrLoadENV(ref JobEnvironment env, string key, lazy string value)
{
    if(thisProcessType() == ChildProcessType.SUBMITTER
    || thisProcessType() == ChildProcessType.ANALYZER)
    {
        auto v = value;
        env.envs[key] = v;
        return v;
    }
    else if(thisProcessType() == ChildProcessType.TASK_MANAGER
         || thisProcessType() == ChildProcessType.TASK_PROCESSOR)
    {
        return environment[key];
    } else {
        return value;
    }
}


/**
qsubを用いてジョブの終了を監視するジョブを投げます．
この関数は，投入した監視ジョブのジョブID(.jb付き)を返します．
*/
string submitMonitoringJob(string jobId, string qname, ClusterInfo cluster)
{
    string dstJobId;
    runOnlyMainProcess({
        if(jobId.canFind('[')) jobId = jobId.split('[')[0];
        auto pipes = pipeProcess(["qsub"], Redirect.stdin | Redirect.stdout);
        scope(exit) wait(pipes.pid);
        scope(failure) kill(pipes.pid);

        {
            auto writer = pipes.stdin.lockingTextWriter;
            .put(writer, "#!/bin/bash\n");
            if(auto kyotobInfo = cast(KyotoBInfo)cluster) {
                writer.formattedWrite("#QSUB -q %s\n", qname);
                .put(writer, "#QSUB -ug gr10061\n");
                .put(writer, "#QSUB -W 336:00\n");
                .put(writer, "#QSUB -A p=1:t=1:c=1:m=3413M\n");
                .put(writer, "\n");
                .put(writer, "cd $QSUB_WORKDIR\n");
            } else {
                writer.formattedWrite("#PBS -q %s\n", qname);
                .put(writer, "#PBS -l walltime=%336:00:00\n");
                .put(writer, "#PBS -l nodes=1:ppn=1\n");
                .put(writer, "\n");
                .put(writer, "cd $PBS_O_WORKDIR\n");
            }
            .put(writer, "set -x\n");
            writer.formattedWrite(monitoringJobScript, jobId);
        }
        pipes.stdin.flush();
        pipes.stdin.close();
        wait(pipes.pid);
        dstJobId = pipes.stdout.byLine.front.chomp.to!string;
    });

    writefln!"JobID Of Monitor:  %s"(dstJobId);

    return dstJobId;
}


static immutable string monitoringJobScript =
q{
target_id="%1$s"

while :
do
    ret=$(qstat | grep $target_id)
    if [ ! "$ret" ]; then
        echo "Finished"
        break
    fi

    sleep 5s
done
};
