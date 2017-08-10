module tuthpc.taskqueue;

import tuthpc.hosts;
import tuthpc.constant;

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


enum bool isTaskList(TL) = is(typeof((TL taskList){
    foreach(i; 0 .. taskList.length){
        taskList[i]();
    }
}));


final class MultiTaskList
{
    this() {}


    this(TL)(TL taskList)
    if(isTaskList!TL)
    {
        this ~= taskList;
    }


    void delegate() opIndex(size_t idx)
    {
        return _tasks[idx];
    }


    size_t length() const @property { return _tasks.length; }


    void opOpAssign(string op : "~", TL)(TL taskList)
    if(isTaskList!TL)
    {
        foreach(i; 0 .. taskList.length){
            this.append(function(typeof(taskList[0]) fn){ fn(); }, taskList[i]);
        }
    }


  private:
    void delegate()[] _tasks;
}


void append(F, T...)(MultiTaskList list, F func, T args)
{
    list._tasks ~= delegate() { func(args); };
}


void append(alias func, T...)(MultiTaskList list, T args)
{
    list._tasks ~= delegate() { func(args); };
}


unittest
{
    static assert(isTaskList!MultiTaskList);

    int a = -1;
    auto taskList = new MultiTaskList(
        [
            { a = 0; },
            { a = 1; },
            { a = 2; },
            { a = 3; },
        ]);

    assert(taskList.length == 4);
    taskList[0]();
    assert(a == 0);
    taskList[1]();
    assert(a == 1);
    taskList[2]();
    assert(a == 2);
    taskList[3]();
    assert(a == 3);

    taskList.append((int b){ a = b; }, 4);
    assert(taskList.length == 5);
    taskList[4]();
    assert(a == 4);

    taskList.append!(b => a = b)(5);
    assert(taskList.length == 6);
    taskList[5]();
    assert(a == 5);
}

unittest
{
    import std.algorithm;
    import std.range;

    int a;
    auto taskList = new MultiTaskList();
    taskList ~= iota(5).map!(i => (){ a = i; });

    assert(taskList.length == 5);
}


final class UniqueTaskAppender(Args...)
{
    this(void delegate(Args) dg)
    {
        _dg = dg;
    }


    this(void function(Args) fp)
    {
        _dg = toDelegate(fp);
    }


    void append(Args args)
    {
        ArgsType a = ArgsType(args);
        if(a !in _set){
            _list ~= a;
            _set[a] = true;
        }
    }


    alias put = append;


    auto opIndex(size_t idx)
    {
        Caller dst = {_dg, _list[idx]};
        return dst;
    }


    size_t length() const @property { return _list.length; }


  private:
    void delegate(Args) _dg;
    ArgsType[] _list;
    bool[ArgsType] _set;

    static struct ArgsType { Args args; }
    static struct Caller
    {
        void delegate(Args) _dg;
        ArgsType _args;

        void opCall(){ _dg(_args.args); }
    }
}


auto uniqueTaskAppender(Args...)(void delegate(Args) dg) { return new UniqueTaskAppender!Args(dg); }


auto uniqueTaskAppender(Args...)(void function(Args) fp) { return new UniqueTaskAppender!Args(fp); }


unittest
{
    int[][int] arrAA;

    auto app = uniqueTaskAppender((int a){ arrAA[a] ~= a; });

    .put(app, iota(10).chain(iota(10)));
    assert(app.length == 10);
    foreach(i; 0 .. app.length)
        app[i]();

    foreach(k, e; arrAA)
        assert(e.length == 1);
}


unittest
{
    struct S { int f; }
    struct ComplexData
    {
        int b;
        long c;
        string d;
        int[] e;
        S s;
    }

    ComplexData[] args;
    foreach(i; 0 .. 10){
        ComplexData data;
        data.b = 1;
        data.c = 2;
        data.d = "foo";
        data.e = new int[3];
        data.s.f = 3;
        args ~= data;
    }

    auto app = uniqueTaskAppender(function(ComplexData d){  });
    .put(app, args);

    assert(app.length == 1);
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
    bool isEnabledTimeCommand = false;  /// timeコマンドをつけるかどうか
    uint ppn = 1;               /// 各ノードで何個のプロセッサを使用するか
    uint nodes = 1;             /// 1つのジョブあたりで実行するノード数
    int mem = -1;               /// 0なら自動設定
    int pmem = -1;              /// 0なら自動設定
    int vmem = -1;              /// 0なら自動設定
    int pvmem = -1;             /// 0なら自動設定
    bool isEnabledEmailOnError = false;  /// エラー時にメールで通知するかどうか
    bool isEnabledEmailOnStart = false; /// 実行開始時にメールで通知するかどうか
    bool isEnabledEmailOnEnd = false;   /// 実行終了時にメールで通知するかどうか
    string[] emailAddrs;                /// メールを送りたい宛先
    bool isEnabledEmailByMailgun = false;   /// エラー時のメールをMailgunで配送するか
    uint maxArraySize = 2048;            /// アレイジョブでの最大のサイズ


    void applyDefaults(Cluster cluster)
    {
        if(queueName is null) queueName = clusters[cluster].queueName;
        if(ppn == 0) ppn = clusters[cluster].maxPPN;
        if(nodes == 0) nodes = clusters[cluster].maxNode;

        immutable maxMem = clusters[cluster].maxMem / clusters[cluster].maxPPN * ppn,
                  maxMemPerPS = maxMem / ppn;

        if(mem == 0) mem = maxMem;
        if(pmem == 0) pmem = maxMemPerPS;
        if(vmem == 0) vmem = maxMem;
        if(pvmem == 0) pvmem = maxMemPerPS;

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

            if(!isEnabledRenameExeFile)
                jobScript = [(bStartsWithDOTSLASH ? "./" : "") ~ originalExeName];
            else
                jobScript = [(bStartsWithDOTSLASH ? "./" : "") ~ renamedExeName];

            if(isEnabledTimeCommand)
                jobScript[0] = "time " ~ jobScript[0];

            jobScript ~= "echo $?";
        }

        if(isEnabledEmailOnStart || isEnabledEmailOnEnd || isEnabledEmailOnError) {
            if(emailAddrs.length == 0){
                string username = environment.get("USER", null);
                enforce(username !is null, "Cannot find USER in ENV.");
                emailAddrs = [username ~ "@edu.tut.ac.jp"];
            }
        }
    }


    void opAssign(in JobEnvironment rhs)
    {
        useArrayJob = rhs.useArrayJob;
        scriptPath = rhs.scriptPath;
        queueName = rhs.queueName;
        dependentJob = rhs.dependentJob;
        unloadModules = rhs.unloadModules.dup;
        loadModules = rhs.loadModules.dup;
        foreach(k, v; rhs.envs) envs[k] = v;
        isEnabledRenameExeFile = rhs.isEnabledRenameExeFile;
        originalExeName = rhs.originalExeName;
        renamedExeName = rhs.renamedExeName;
        prescript = rhs.prescript.dup;
        jobScript = rhs.jobScript.dup;
        postscript = rhs.postscript.dup;
        ppn = rhs.ppn;
        nodes = rhs.nodes;
        mem = rhs.mem;
        pmem = rhs.pmem;
        vmem = rhs.vmem;
        pvmem = rhs.pvmem;
        isEnabledEmailOnError = rhs.isEnabledEmailOnError;
        isEnabledEmailOnStart = rhs.isEnabledEmailOnStart;
        isEnabledEmailOnEnd = rhs.isEnabledEmailOnEnd;
        emailAddrs = emailAddrs.dup;
        isEnabledEmailByMailgun = rhs.isEnabledEmailByMailgun;
        isEnabledTimeCommand = rhs.isEnabledTimeCommand;
    }
}


void makeQueueScript(R)(ref R orange, Cluster cluster, in JobEnvironment env_, size_t jobCount = 1)
{
    JobEnvironment jenv;
    jenv = env_;

    jenv.applyDefaults(cluster);

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
    orange.formattedWrite("#PBS -l nodes=%s:ppn=%s", jenv.nodes, jenv.ppn);
    if(jenv.mem != -1) orange.formattedWrite(",mem=%sgb", jenv.mem);
    if(jenv.pmem != -1) orange.formattedWrite(",pmem=%sgb", jenv.pmem);
    if(jenv.vmem != -1) orange.formattedWrite(",vmem=%sgb", jenv.vmem);
    if(jenv.pvmem != -1) orange.formattedWrite(",pvmem=%sgb", jenv.pvmem);
    .put(orange, '\n');
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


PushResult run(TL)(TL taskList, JobEnvironment env = JobEnvironment.init, string file = __FILE__, size_t line = __LINE__)
if(isTaskList!TL)
{
    import std.ascii;

    env.useArrayJob = true;

    string dstJobId;
    immutable nowInRunOld = RunState.nowInRun;

    RunState.nowInRun = true;
    scope(exit){
        RunState.nowInRun = nowInRunOld;
        if(!RunState.nowInRun)
            ++RunState.countOfCallRun;
    }


    size_t arrayJobSize = taskList.length;
    if(arrayJobSize > env.maxArraySize)
            arrayJobSize = env.maxArraySize;

    if(nowRunningOnClusterDevelopmentHost()){
        auto cluster = loginCluster();

        enforce(nowInRunOld == false);
        env.envs["JOB_ENV_TUTHPC_RUN_ID"] = RunState.countOfCallRun.to!string;
        env.envs["JOB_ENV_TUTHPC_ID"] = "${PBS_ARRAYID}";

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
            writeln("ID: ", dstJobId);
        }
    }else if(nowRunningOnClusterComputingNode() && nowInRunOld == false){
        auto cluster = Cluster.wdev;
        env.applyDefaults(cluster);

        auto environmentAA = environment.toAA();
        enforce(
                "JOB_ENV_TUTHPC_RUN_ID" in environmentAA
             && "JOB_ENV_TUTHPC_ID" in environmentAA, "cannot find environment variables: 'JOB_ENV_TUTHPC_RUN_ID', and 'JOB_ENV_TUTHPC_ID'");

        if(environmentAA["JOB_ENV_TUTHPC_RUN_ID"].to!size_t == RunState.countOfCallRun)
        {
            size_t index = environmentAA["JOB_ENV_TUTHPC_ID"].to!size_t();
            enforce(index < arrayJobSize);

            for(; index < taskList.length; index += env.maxArraySize){
                if(auto ex = collectException!Throwable(taskList[index]())){
                    scope(exit)
                        throw ex;

                    if(env.isEnabledEmailOnError && env.isEnabledEmailByMailgun) {
                        import std.datetime;
                        import std.conv;
                        import std.socket;
                        string[2][] info;
                        auto jobid = environmentAA.get("PBS_JOBID", "Unknown");
                        info ~= ["PBS_JOBID",           jobid];
                        info ~= ["FileName",            file];
                        info ~= ["Line",                line.to!string];
                        info ~= ["JOB_ENV_TUTHPC_RUN_ID", environmentAA["JOB_ENV_TUTHPC_RUN_ID"]];
                        info ~= ["JOB_ENV_TUTHPC_ID",   index.to!string];
                        info ~= ["PBS_ARRAYID",         environmentAA.get("PBS_ARRAYID", "Unknown")];
                        info ~= ["Job size",            taskList.length.to!string];
                        info ~= ["End time",            Clock.currTime.toISOExtString()];
                        info ~= ["Host",                Socket.hostName()];
                        info ~= ["Exception",           "\n" ~ ex.to!string];

                        enforce("MAILGUN_APIKEY" in environmentAA
                            &&  "MAILGUN_DOMAIN" in environmentAA);

                        auto apikey = environmentAA["MAILGUN_APIKEY"];
                        auto domain = environmentAA["MAILGUN_DOMAIN"];

                        import std.net.curl;
                        import std.uri;
                        auto http = HTTP("api.mailgun.net");
                        http.setAuthentication("api", apikey);
                        std.net.curl.post("https://api.mailgun.net/v3/%s/messages".format(domain),
                                ["from": "TUTHPCLib <mailgun@%s>".format(domain),
                                 "to": env.emailAddrs.join(','),
                                 "subject": "Error on Job %s".format(jobid),
                                 "text": "%(%-(%s: \t%)\n%)".format(info)],
                                 http
                            );
                    }
                }
            }
        }
    }else if(nowInRunOld == true){
        foreach(i; 0 .. taskList.length)
            taskList[i]();
    }
    else{
        import std.parallelism;
        foreach(i; iota(taskList.length).parallel)
            taskList[i]();
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
