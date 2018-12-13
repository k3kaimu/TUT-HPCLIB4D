module tuthpc.taskqueue;

import core.runtime;

import tuthpc.mail;
import tuthpc.hosts;
import tuthpc.constant;
public import tuthpc.tasklist;
import tuthpc.limiter;

import std.algorithm;
import std.range;
import std.format;
import std.conv;
import std.process;
import std.stdio;
import std.exception;
import std.digest.crc;
import std.traits;
import std.functional;
import std.datetime;
import std.path;


enum EnvironmentKey : string
{
    RUN_ID = "TUTHPC_JOB_ENV_RUN_ID",
    ARRAY_ID = "TUTHPC_JOB_ENV_ARRAY_ID",
    TASK_ID = "TUTHPC_JOB_ENV_TASK_ID",
    PBS_JOBID = "PBS_JOBID",
}


string hashOfExe()
{
    import std.file;

    return crc32Of(cast(ubyte[])std.file.read(Runtime.args[0])).toHexString.dup;
}


string lockFileNameForLimitProcess()
{
    auto envs = environment.toAA();
    if("PBS_JOBID" in envs && "PBS_ARRAYID" in envs)
        return format("%s_%s", envs["PBS_JOBID"], envs["PBS_ARRAYID"]);
    else
        return hashOfExe();
}


enum DependencySetting
{
    success = "afterokarray",
    failure = "afternotokarray",
    exit = "afteranyarray",
}


struct JobEnvironment
{
    bool useArrayJob = true;    /// ArrayJobにするかどうか
    bool useArgs = true;
    string scriptPath;          /// スクリプトファイルの保存場所, nullならパイプでqsubにジョブを送る
    string queueName;           /// nullのとき，自動でclusters[cluster].queueNameに設定される
    string dependentJob;        /// 依存しているジョブのID
    string dependencySetting = DependencySetting.exit;   /// 依存しているジョブがどの状況でこのジョブを実行するか
    string[] unloadModules;     /// module unloadで破棄されるモジュール
    string[] loadModules;       /// module loadで読み込まれるモジュール
    string[string] envs;        /// 環境変数
    bool isEnabledRenameExeFile = true; /// 実行ジョブファイル名を，バイナリ表現にもとづきユニークな名前に変更します．
    string originalExeName;     /// リネーム・コピー前の実行ファイル名
    string renamedExeName;      /// リネーム・コピー後の実行ファイル名
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
    bool isEnabledQueueOverflowProtection = true;   /// キューの最大値(4096)以上ジョブを投入しないようにする
    //bool isEnabledSpawnNewProcess = true;   /// 各タスクは新しいプロセスを起動する
    size_t totalProcessNum = 50;
    string logdir;              /// 各タスクの標準出力，標準エラーを保存するディレクトリを指定できる


    void applyDefaults(Cluster cluster)
    {
        if(useArgs){
            import std.getopt;
            auto args = Runtime.args;
            int walltime_int = -1;

            getopt(args,
                std.getopt.config.passThrough,
                "th:queue|th:q", &queueName,
                "th:after|th:a", &dependentJob,
                "th:ppn|th:p", &ppn,
                "th:nodes|th:n", &nodes,
                "th:taskGroupSize|th:g", &taskGroupSize,
                "th:walltime|th:w", &walltime_int,
                "th:mailOnError|th:me", &isEnabledEmailOnError,
                "th:mailOnStart|th:ms", &isEnabledEmailOnStart,
                "th:mailOnFinish|th:mf", &isEnabledEmailOnEnd,
                "th:mailTo", &emailAddrs,
                "th:maxArraySize|th:m", &maxArraySize,
                "th:queueOverflowProtection|th:qop", &isEnabledQueueOverflowProtection,
                "th:maxProcessNum|th:plim", &totalProcessNum,
                "th:logdir", &logdir
            );

            if(walltime_int != -1)
                walltime = walltime_int.hours;
        }

        if(queueName is null) queueName = clusters[cluster].queueName;
        if(ppn == 0) ppn = clusters[cluster].maxPPN;
        if(nodes == 0) nodes = clusters[cluster].maxNode;

        if(nodes == 1 && ppn == 1 && taskGroupSize == 0){
            taskGroupSize = 11;
        }else if(nodes != 1 || ppn != 1){
            taskGroupSize = 1;
        }

        int maxMem = 5 * (ppn * taskGroupSize),
            maxMemPerPS = maxMem / (ppn * taskGroupSize);

        if(!queueName.endsWith("rchq")){
            maxMem = 5;
            maxMemPerPS = 5;
        }

        if(mem == 0) mem = maxMem;
        if(pmem == 0) pmem = maxMemPerPS;
        if(vmem == 0) vmem = maxMem;
        if(pvmem == 0) pvmem = maxMemPerPS;

        if(walltime == 0.seconds){
            switch(queueName){
                case "wLrchq":
                    walltime = 335.hours;
                    break;
                default:
                    walltime = 1.hours;
            }
        }

        if(isEnabledEmailOnStart || isEnabledEmailOnEnd || isEnabledEmailOnError) {
            if(emailAddrs.length == 0){
                string username = environment.get("USER", null);
                enforce(username !is null, "Cannot find USER in ENV.");
                emailAddrs = [username ~ "@edu.tut.ac.jp"];
            }
        }

        import core.runtime;
        if(jobScript is null){
            originalExeName = Runtime.args[0];
            bool bStartsWithDOTSLASH = false;
            while(originalExeName.startsWith("./")){
                bStartsWithDOTSLASH = true;
                originalExeName = originalExeName[2 .. $];
            }

            if(isEnabledRenameExeFile && renamedExeName.walkLength == 0){
                import std.file;
                renamedExeName = format("%s_%s", originalExeName, crc32Of(cast(ubyte[])std.file.read(originalExeName)).toHexString);
            }

            if(!isEnabledRenameExeFile && renamedExeName.walkLength == 0){
                renamedExeName = originalExeName;
            }

            jobScript = [format("%s %-('%s'%| %)", (bStartsWithDOTSLASH ? "./" : "") ~ renamedExeName, Runtime.args[1 .. $])];

            if(isEnabledEmailOnError){
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
    }


    void opAssign(in JobEnvironment rhs)
    {
        foreach(i, ref e; this.tupleof){
            static if(is(typeof(e) : ulong) || is(typeof(e) : string) || is(typeof(e) : Duration)){
                e = rhs.tupleof[i];
            }else static if(is(typeof(e) : string[]) || is(typeof(e) : string[string])){
                e = null;
                foreach(k, v; rhs.tupleof[i])
                    e[k] = v;
            }else
                static assert(0);
        }
    }
}


void makeQueueScript(R)(ref R orange, Cluster cluster, in JobEnvironment jenv, size_t jobCount = 1)
{
    if(jenv.isEnabledRenameExeFile){
        import std.file;

        // check that the renamed file already exists
        if(!exists(jenv.renamedExeName)) {
            writefln("copy: %s -> %s", jenv.originalExeName, jenv.renamedExeName);
            std.file.copy(jenv.originalExeName, jenv.renamedExeName);
            enforce(execute(["chmod", "+x", jenv.renamedExeName]).status == 0);
        }
    }

    .put(orange, "#!/bin/bash\n");
    orange.formattedWrite("#PBS -l nodes=%s:ppn=%s", jenv.nodes, jenv.ppn * jenv.taskGroupSize);
    if(jenv.mem != -1) orange.formattedWrite(",mem=%sgb", jenv.mem);
    if(jenv.pmem != -1) orange.formattedWrite(",pmem=%sgb", jenv.pmem);
    if(jenv.vmem != -1) orange.formattedWrite(",vmem=%sgb", jenv.vmem);
    if(jenv.pvmem != -1) orange.formattedWrite(",pvmem=%sgb", jenv.pvmem);

    .put(orange, '\n');

    {
        int hrs, mins, secs;
        jenv.walltime.split!("hours", "minutes", "seconds")(hrs, mins, secs);
        orange.formattedWrite("#PBS -l walltime=%d:%02d:%02d\n", hrs, mins, secs);
    }

    orange.formattedWrite("#PBS -q %s\n", jenv.queueName);

    if(jenv.dependentJob.length != 0){
        orange.formattedWrite("#PBS -W depend=%s:%s\n", cast(string)jenv.dependencySetting, jenv.dependentJob);
    }

    if(jenv.useArrayJob) orange.formattedWrite("#PBS -t %s-%s\n", 0, jobCount-1);

    if(jenv.isEnabledEmailOnStart || jenv.isEnabledEmailOnEnd || jenv.isEnabledEmailOnError) {
        .put(orange, "#PBS -m ");
        if(jenv.isEnabledEmailOnStart)  .put(orange, 'b');
        if(jenv.isEnabledEmailOnEnd)    .put(orange, 'e');
        if(jenv.isEnabledEmailOnError)  .put(orange, 'a');
        .put(orange, '\n');
        orange.formattedWrite("#PBS -M %-(%s %)\n", jenv.emailAddrs);
    }

    .put(orange, "set -e\n");
    .put(orange, "source ~/.bashrc\n");
    .put(orange, "MPI_PROCS=`wc -l $PBS_NODEFILE | awk '{print $1}'`\n");
    .put(orange, "cd $PBS_O_WORKDIR\n");

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


struct PushResult
{
    string jobId;
}


struct QueueOverflowProtector
{
    static int countOfEnqueuedJobs = -1;
    static int countOfTotalMyJobs = 0;
    static bool alreadySpawnAnalyzer = false;


    static
    bool isAnalyzerProcess()
    {
        return environment.get("TUTHPC_ANALYZE_ALL_JOB") !is null;
    }


    static
    void analyze(size_t jobSize, size_t runId, string file, size_t line)
    {
        enforce(nowRunningOnClusterDevelopmentHost());

        if(isAnalyzerProcess){
            if(countOfEnqueuedJobs == -1){
                countOfEnqueuedJobs = tuthpc.hosts.countOfEnqueuedJobs();
                enforce(countOfEnqueuedJobs != -1, "Cannot get the number of enqueued jobs.");
            }

            countOfTotalMyJobs += jobSize;
            writefln("ANALYZE: %s jobs are spawned on %s(%s)[%s]", jobSize, file, line, runId);
            enforce(countOfTotalMyJobs + countOfEnqueuedJobs < 16000,
                "Your jobs may cause queue overflow. Please use env.maxArraySize.");
        }else{
            if(!alreadySpawnAnalyzer){
                string cmd = format("%s %-('%s'%| %)", Runtime.args[0], Runtime.args[1 .. $]);
                writefln("The job analyzer is spawned with \"%s\".", cmd);

                auto analyzer = executeShell(cmd, ["TUTHPC_ANALYZE_ALL_JOB" : "TRUE"]);
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


Pid spawnTask(JobEnvironment jenv, size_t taskIndex, string logdir, string file, size_t line, string[string] addEnvs = null)
{
    import std.path;

    if(nowRunningOnClusterDevelopmentHost() || nowRunningOnClusterComputingNode())
        enforce(numOfProcessOfUser() <= jenv.totalProcessNum, "Many processes are spawned.");

    string outname = buildPath(logdir, format("stdout_%s.log", taskIndex));
    string errname = buildPath(logdir, format("stderr_%s.log", taskIndex));

    string[string] env = [EnvironmentKey.TASK_ID : taskIndex.to!string];
    foreach(k, v; addEnvs) env[k] = v;

    auto pid = spawnProcess(Runtime.args, std.stdio.stdin, File(outname, "w"), File(errname, "w"), env);
    return pid;
}


void loggingTask(int status, JobEnvironment jenv, size_t taskIndex, string logdir, string file, size_t line)
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
        writer.formattedWrite("=========== END OF %sTH TASK. (status = %s) ===========\n", taskIndex, status);
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

    if(status != 0 && jenv.isEnabledEmailOnError){
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


void processTasks(R, TL)(JobEnvironment jenv, uint parallelSize, R taskIndxs, TL taskList, string logdir, string file, size_t line, string[string] addEnvs = null)
{
    import std.path;
    import std.process;
    import std.file;

    if(auto strOfTaskID = environment.get(EnvironmentKey.TASK_ID)){
        immutable size_t taskIndex = strOfTaskID.to!size_t();

        enforce(environment.get(EnvironmentKey.RUN_ID, "-1").to!long == RunState.countOfCallRun);

        import core.thread;
        try {   // 環境によっては失敗する可能性がある（WSL上のUbuntuでは例外送出）
            Thread.getThis.priority = Thread.PRIORITY_MIN;
        } catch(core.thread.ThreadException ex){
            //stderr.writeln(ex);
        }

        taskList[taskIndex]();
    }else{
        enforce(exists(logdir));

        static struct ProcessState
        {
            Pid pid;
            size_t taskIndex;
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
                    proc = new ProcessState(spawnTask(jenv, taskIndex, logdir, file, line, addEnvs), taskIndex);
                }

                if(proc !is null) {
                    auto status = tryWait(proc.pid);
                    if(status.terminated) {
                        loggingTask(status.status, jenv, proc.taskIndex, logdir, file, line);
                        proc = null;
                    }
                }
            }

            import core.thread;
            Thread.sleep(1.seconds);
        }
    }
}


PushResult run(TL)(TL taskList, JobEnvironment env = JobEnvironment.init, string file = __FILE__, size_t line = __LINE__)
if(isTaskList!TL)
{
    import std.ascii;

    auto cluster = Cluster.wdev;
    env.useArrayJob = true;

    if(env.nodes == 1 && env.ppn == 1 && env.taskGroupSize == 0){
        if(taskList.length <= 40)
            env.taskGroupSize = 1;
        else if(taskList.length < 220)
            env.taskGroupSize = 7;
        else
            env.taskGroupSize = 11;
    }

    env.applyDefaults(cluster);

    PushResult result;
    immutable nowInRunOld = RunState.nowInRun;

    RunState.nowInRun = true;
    scope(exit){
        RunState.nowInRun = nowInRunOld;
        if(!RunState.nowInRun)
            ++RunState.countOfCallRun;
    }

    size_t arrayJobSize = taskList.length / env.taskGroupSize + (taskList.length % env.taskGroupSize != 0 ? 1 : 0);
    if(arrayJobSize > env.maxArraySize)
        arrayJobSize = env.maxArraySize;

    immutable logdir = logDirName(env, RunState.countOfCallRun);

    if(nowRunningOnClusterDevelopmentHost())
    {
        enforce(nowInRunOld == false);
        enforce(env.useArrayJob);

        if(nowRunningOnClusterDevelopmentHost() && env.isEnabledQueueOverflowProtection){
            QueueOverflowProtector.analyze(arrayJobSize, RunState.countOfCallRun, file, line);

            if(QueueOverflowProtector.isAnalyzerProcess)
                goto Lreturn;
        }

        import std.file : mkdir;
        mkdir(logdir);

        // ジョブを投げる
        result = pushArrayJobToQueue(
            RunState.countOfCallRun.to!string,
            arrayJobSize, env, cluster,
            file, line);

        writeln("ID: ", result.jobId);
        writefln("\ttaskList.length: %s", taskList.length);
        writefln("\tArray Job Size: %s", arrayJobSize);
        writefln("\tLog directory: %s", logdir);
    }
    else if(nowRunningOnClusterComputingNode() && nowInRunOld == false)
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

            processTasks(env, env.taskGroupSize, taskIndexList, taskList, logdir, file, line);
        }
    }
    else if(nowInRunOld == true)
    {
        foreach(i; 0 .. taskList.length)
            taskList[i]();
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
        }

        uint parallelSize = std.parallelism.totalCPUs / env.ppn;
        processTasks(env, parallelSize, iota(taskList.length), taskList, logdir, file, line, [EnvironmentKey.RUN_ID : RunState.countOfCallRun.to!string]);
    }

  Lreturn:
    return result;
}


private
PushResult pushArrayJobToQueue(string runId, size_t arrayJobSize, JobEnvironment env, Cluster cluster, string file, size_t line)
{
    env.envs[EnvironmentKey.RUN_ID] = runId;
    env.envs[EnvironmentKey.ARRAY_ID] = "${PBS_ARRAYID}";

    string dstJobId;

    if(env.scriptPath !is null){
        auto app = appender!string;

        makeQueueScript(app, cluster, env, arrayJobSize);

        import std.file;
        std.file.write(env.scriptPath, app.data);

        auto qsub = execute(["qsub", env.scriptPath]);
        dstJobId = qsub.output.until!(a => a != '.').array().to!string;
    }else{
        auto pipes = pipeProcess(["qsub"], Redirect.stdin | Redirect.stdout);
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

    return PushResult(dstJobId);
}


template afterRunImpl(DependencySetting ds)
{
    PushResult afterRunImpl(TL)(PushResult parentJob, TL taskList, JobEnvironment env = JobEnvironment.init, string file = __FILE__, size_t line = __LINE__)
    if(isTaskList!TL)
    {
        env.dependentJob = parentJob.jobId;
        env.dependencySetting = ds;
        return run(taskList, env, file, line);
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
            return range.map!(a => { fn(a); });
        else
            return range.array().map!(a => { fn(a); });
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


auto runAsTasks(R)(R range, JobEnvironment env = JobEnvironment.init, string file = __FILE__, size_t line = __LINE__)
if(isInputRange!R)
{
    static struct RunAsTasksResult
    {
        alias E = ElementType!R;

        int opApply(int delegate(ref E) dg)
        {
            //int result = 0;
            MultiTaskList taskList = new MultiTaskList();
            for(size_t i = 0; !_range.empty;){
                taskList.append(dg, _range.front);
                ++i;
                _range.popFront;
            }

            .run(taskList, _env, _filename, _line);

            return 0;
        }


        int opApply(int delegate(ref size_t, ref E) dg)
        {
            MultiTaskList taskList = new MultiTaskList();
            for(size_t i = 0; !_range.empty;){
                taskList.append(dg, i, _range.front);
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


    return RunAsTasksResult(range, env, file, line);
}


auto appendAsTasks(R)(R range, MultiTaskList taskList)
if(isInputRange!R)
{
    static struct AppendAsTasksResult
    {
        alias E = ElementType!R;

        int opApply(int delegate(ref E) dg)
        {
            for(size_t i = 0; !_range.empty;){
                _taskList.append(dg, _range.front);
                ++i;
                _range.popFront;
            }

            return 0;
        }


        int opApply(int delegate(ref size_t, ref E) dg)
        {
            for(size_t i = 0; !_range.empty;){
                _taskList.append(dg, i, _range.front);
                ++i;
                _range.popFront;
            }

            return 0;
        }

      private:
        R _range;
        MultiTaskList _taskList;
    }


    return AppendAsTasksResult(range, taskList);
}
