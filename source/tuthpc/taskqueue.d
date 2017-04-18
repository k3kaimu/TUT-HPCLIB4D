module tuthpc.taskqueue;

import tuthpc.hosts;
import tuthpc.constant;

import std.range;
import std.format;
import std.conv;
import std.process;
import std.stdio;
import std.exception;


final class MultiTaskList
{
    this() {}
    this(void delegate()[] list) { _tasks = list; }

    void append(F, T...)(F func, T args)
    {
        _tasks ~= delegate() { func(args); };
    }


    void delegate() opIndex(size_t idx)
    {
        return _tasks[idx];
    }


    size_t length() const @property { return _tasks.length; }


  private:
    void delegate()[] _tasks;
}


struct JobEnvironment
{
    bool useArrayJob = true;    /// ArrayJobにするかどうか
    string scriptPath;          /// スクリプトファイルの保存場所, nullならパイプでqsubにジョブを送る
    string queueName;           /// nullのとき，自動でclusters[cluster].queueNameに設定される
    string[] unloadModules;     /// module unloadで破棄されるモジュール
    string[] loadModules;       /// module loadで読み込まれるモジュール
    string[string] envs;        /// 環境変数
    string[] prescript;         /// プログラム実行前に実行されるシェルスクリプト
    string[] jobScript;         /// ジョブスクリプト
    string[] postscript;        /// プログラム実行後に実行されるシェルスクリプト
    uint ppn = 1;               /// 各ノードで何個のプロセッサを使用するか
    uint nodes = 1;             /// 1つのジョブあたりで実行するノード数
    int mem = -1;               /// 0なら自動設定
    int pmem = -1;              /// 0なら自動設定
    int vmem = -1;              /// 0なら自動設定
    int pvmem = -1;             /// 0なら自動設定


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
        if(jobScript is null)
            jobScript = [Runtime.args[0]];
    }


    void opAssign(in JobEnvironment rhs)
    {
        useArrayJob = rhs.useArrayJob;
        scriptPath = rhs.scriptPath;
        queueName = rhs.queueName;
        unloadModules = rhs.unloadModules.dup;
        loadModules = rhs.loadModules.dup;
        prescript = rhs.prescript.dup;
        jobScript = rhs.jobScript.dup;
        postscript = rhs.postscript.dup;
        foreach(k, v; rhs.envs) envs[k] = v;
        ppn = rhs.ppn;
        nodes = rhs.nodes;
        mem = rhs.mem;
        pmem = rhs.pmem;
        vmem = rhs.vmem;
        pvmem = rhs.pvmem;
    }
}


void makeQueueScript(R)(ref R orange, Cluster cluster, in JobEnvironment env_, size_t jobCount = 1)
{
    JobEnvironment jenv;
    jenv = env_;

    jenv.applyDefaults(cluster);

    .put(orange, "#!/bin/bash\n");
    orange.formattedWrite("#PBS -l nodes=%s:ppn=%s", jenv.nodes, jenv.ppn);
    if(jenv.mem != -1) orange.formattedWrite(",mem=%sgb", jenv.mem);
    if(jenv.pmem != -1) orange.formattedWrite(",pmem=%sgb", jenv.pmem);
    if(jenv.vmem != -1) orange.formattedWrite(",vmem=%sgb", jenv.vmem);
    if(jenv.pvmem != -1) orange.formattedWrite(",pvmem=%sgb", jenv.pvmem);
    .put(orange, '\n');
    orange.formattedWrite("#PBS -q %s\n", jenv.queueName);
    if(jenv.useArrayJob) orange.formattedWrite("#PBS -t %s-%s\n", 0, jobCount-1);
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


void pushArrayJob(MultiTaskList taskList, JobEnvironment env, string file = __FILE__, size_t line = __LINE__)
{
    env.useArrayJob = true;

    if(nowRunningOnClusterDevelopmentHost()){
        auto app = appender!string;
        auto cluster = loginCluster();

        env.envs["JOB_ENV_TUTHPC_FILE"] = file;
        env.envs["JOB_ENV_TUTHPC_LINE"] = line.to!string;
        env.envs["JOB_ENV_TUTHPC_ID"] = "${PBS_ARRAYID}";

        if(env.scriptPath !is null){
            import std.file;
            std.file.write("pushToQueue.sh", app.data);

            auto qsub = execute(["qsub", "pushToQueue.sh"]);
            writeln(qsub.status == 0 ? "Successed push to queue" : "Failed push to queue");
            writeln("qsub output: ", qsub.output);
        }else{
            auto pipes = pipeProcess(["qsub"], Redirect.stdin);
            scope(exit) wait(pipes.pid);

            {
                auto writer = pipes.stdin.lockingTextWriter;
                makeQueueScript(writer, cluster, env, taskList.length);
            }
            pipes.stdin.flush();
            pipes.stdin.close();
        }
    }else if(nowRunningOnClusterComputingNode()){
        auto envs = environment.toAA();
        enforce("JOB_ENV_TUTHPC_LINE" in envs
             && "JOB_ENV_TUTHPC_FILE" in envs
             && "JOB_ENV_TUTHPC_ID" in envs, "cannot find environment variables: 'JOB_ENV_TUTHPC_LINE', 'JOB_ENV_TUTHPC_FILE', and 'JOB_ENV_TUTHPC_ID'");

        if(envs["JOB_ENV_TUTHPC_FILE"] == file
        && envs["JOB_ENV_TUTHPC_LINE"].to!size_t == line)
        {
            auto index = envs["JOB_ENV_TUTHPC_ID"].to!size_t();
            enforce(index < taskList.length);

            taskList[index]();
        }
    }else{
        import std.parallelism;
        foreach(i; iota(taskList.length).parallel)
            taskList[i]();
    }
}


/+
void makeQueueScriptForMPI(R)(ref R orange, Cluster cluster, string[string] envs, string[][] prescripts, string binName, string[][] postscripts,uint nodes, uint ppn = 0)
{
    immutable info = clusters[cluster];

    if(ppn == 0)
        ppn = info.maxPPN;

    if(nodes > info.maxNode)
        nodes = info.maxNode;

    makeQueueScriptHeaderForOpenMPI(orange, cluster, nodes, ppn);
    foreach(i, es; prescripts){
        foreach(e; es){
            .put(orange, es);
            .put(orange, " ");
        }

        .put(orange, '\n');
    }

    string envXs;
    foreach(k, v; envs)
        envXs ~= format("-x %s=%s ", k, v);

    .put(orange, ["mpirun ", envXs, " -np $MPI_PROCS ", binName, "\n"]);
    foreach(i, es; postscripts){
        foreach(e; es){
            .put(orange, es);
            .put(orange, " ");
        }

        .put(orange, '\n');
    }
}


void makeQueueScriptForArrayJob(R)(ref R orange, Cluster cluster, string[string] envs, string[][] prescripts, string binName, string[][] postscripts, uint ppn, size_t jobCount)
{
    immutable info = clusters[cluster];

    if(ppn == 0)
        ppn = info.maxPPN;

    makeQueueScriptHeaderForArrayJob(orange, cluster, ppn, jobCount);
    foreach(i, es; prescripts){
        foreach(e; es){
            .put(orange, es);
            .put(orange, " ");
        }

        .put(orange, '\n');
    }

    string envXs;
    foreach(k, v; envs){
        orange.formattedWrite("export %s=%s\n", k, v);
    }

    .put(orange, ["./" ~ binName, "\n"]);
    foreach(i, es; postscripts){
        foreach(e; es){
            .put(orange, es);
            .put(orange, " ");
        }

        .put(orange, '\n');
    }
}


void makeQueueScriptHeaderForOpenMPI(R)(ref R orange, Cluster cluster, uint nodes, uint ppn)
{
    import std.format : formattedWrite;

    immutable maxMem = clusters[cluster].maxMem / clusters[cluster].maxPPN * ppn,
              maxMemPerPS = maxMem / ppn;

    .put(orange, "#!/bin/bash\n");
    orange.formattedWrite("#PBS -l nodes=%s:ppn=%s,mem=%sgb,pmem=%sgb,vmem=%sgb,pvmem=%sgb\n", nodes, ppn, maxMem, maxMemPerPS, maxMem, maxMemPerPS);
    orange.formattedWrite("#PBS -q %s\n", clusters[cluster].queueName);
    .put(orange, "source ~/.bashrc\n");
    .put(orange, "MPI_PROCS=`wc -l $PBS_NODEFILE | awk '{print $1}'`\n");
    .put(orange, "cd $PBS_O_WORKDIR\n");
    .put(orange, "module unload intelmpi.intel\n");
    .put(orange, "module load openmpi.intel\n");
}


void makeQueueScriptHeaderForArrayJob(R)(ref R orange, Cluster cluster, uint ppn, size_t jobCount)
{
    import std.format : formattedWrite;

    immutable maxMem = clusters[cluster].maxMem / clusters[cluster].maxPPN * ppn,
              maxMemPerPS = maxMem / ppn;

    .put(orange, "#!/bin/bash\n");
    orange.formattedWrite("#PBS -l nodes=1:ppn=%s\n", ppn);
    orange.formattedWrite("#PBS -q %s\n", clusters[cluster].queueName);
    orange.formattedWrite("#PBS -t %s-%s\n", 0, jobCount-1);
    .put(orange, "source ~/.bashrc\n");
    .put(orange, "cd $PBS_O_WORKDIR\n");
    .put(orange, "module unload intelmpi.intel\n");
    .put(orange, "module load openmpi.intel\n");
}


void jobRun(T = string)(uint nodes, uint ppn,
            T id,
            void delegate() dg,
            string file = __FILE__,
            size_t line = __LINE__)
{
    import core.runtime : Runtime;
    import std.format : format, formattedWrite;
    import std.array : appender;
    import std.process : execute, environment;
    import std.stdio : writeln;
    import std.exception : enforce;
    import std.conv : to;

    if(nowRunningOnClusterDevelopmentHost()){
        immutable name = Runtime.args[0];
        auto app = appender!string;

        auto cluster = loginCluster();

        makeQueueScriptForMPI(app, cluster,
                            ["JOB_ENV_TUTHPC_FILE": file,
                             "JOB_ENV_TUTHPC_LINE": line.to!string,
                             "JOB_ENV_TUTHPC_ID": id.to!string],
                            [], name, [], nodes, ppn);

        import std.file;
        std.file.write("pushToQueue.sh", app.data);

        auto qsub = execute(["qsub", "pushToQueue.sh"]);
        writeln(qsub.status == 0 ? "Successed push to queue" : "Failed push to queue");
        writeln("qsub output: ", qsub.output);
    }else if(nowRunningOnClusterComputingNode()){
        auto envs = environment.toAA();
        enforce("JOB_ENV_TUTHPC_LINE" in envs
             && "JOB_ENV_TUTHPC_FILE" in envs
             && "JOB_ENV_TUTHPC_ID" in envs, "cannot find environment variables: 'JOB_ENV_TUTHPC_LINE', 'JOB_ENV_TUTHPC_FILE', and 'JOB_ENV_TUTHPC_ID'");

        if(envs["JOB_ENV_TUTHPC_FILE"] == file
        && envs["JOB_ENV_TUTHPC_LINE"].to!size_t == line
        && envs["JOB_ENV_TUTHPC_ID"] == id.to!string)
        {
            dg();
        }
    }else{
        // execute serial
        dg();
    }
}


void jobRun(uint nodes, uint ppn,
            void delegate() dg,
            string file = __FILE__,
            size_t line = __LINE__)
{
    jobRun(nodes, ppn, "null", dg, file, line);
}


void jobRun(alias func, T = string)(uint nodes = 1, uint ppn = 0,
                        T id = "none",
                        string file = __FILE__,
                        size_t line = __LINE__)
{
    jobRun(nodes, ppn, id, delegate(){ func(); },  file, line);
}


void jobRun(alias func)(uint nodes = 1, uint ppn = 0, string file = __FILE__, size_t line = __LINE__)
{
    jobRun!func(nodes, ppn, "none", file, line);
}


void jobRun(MultiTaskList taskList, string[] prescirpts = null, string[] postscripts = null, uint ppn = 1, string file = __FILE__, size_t line = __LINE__)
{
    import core.runtime : Runtime;
    import std.format : format, formattedWrite;
    import std.array : appender;
    import std.process : execute, environment;
    import std.stdio : writeln;
    import std.exception : enforce;
    import std.conv : to;

    if(nowRunningOnClusterDevelopmentHost()){
        immutable name = Runtime.args[0];
        auto app = appender!string;

        auto cluster = loginCluster();

        makeQueueScriptForArrayJob(app, cluster,
                            ["JOB_ENV_TUTHPC_FILE": file,
                             "JOB_ENV_TUTHPC_LINE": line.to!string,
                             "JOB_ENV_TUTHPC_ID": "${PBS_ARRAYID}"],
                            [], name, [], ppn, taskList.length);

        import std.file;
        std.file.write("pushToQueue.sh", app.data);

        auto qsub = execute(["qsub", "pushToQueue.sh"]);
        writeln(qsub.status == 0 ? "Successed push to queue" : "Failed push to queue");
        writeln("qsub output: ", qsub.output);
    }else if(nowRunningOnClusterComputingNode()){
        auto envs = environment.toAA();
        enforce("JOB_ENV_TUTHPC_LINE" in envs
             && "JOB_ENV_TUTHPC_FILE" in envs
             && "JOB_ENV_TUTHPC_ID" in envs, "cannot find environment variables: 'JOB_ENV_TUTHPC_LINE', 'JOB_ENV_TUTHPC_FILE', and 'JOB_ENV_TUTHPC_ID'");

        if(envs["JOB_ENV_TUTHPC_FILE"] == file
        && envs["JOB_ENV_TUTHPC_LINE"].to!size_t == line)
        {
            auto index = envs["JOB_ENV_TUTHPC_ID"].to!size_t();
            enforce(index < taskList.length);

            taskList[index]();
        }
    }else{
        import std.parallelism;

        foreach(i; iota(taskList.length).parallel)
            taskList[i]();
    }
}


//unittest 
//{
//    import std.stdio;
//    auto app = appender!string();
//    makeQueueScriptForMPI(app, Cluster.wdev,
//            [["aaaaa", "bbbbb"], ["cccc", "ddddd"]],
//            "bash",
//            [["aaaaa", "bbbbb"], ["cccc", "ddddd"]],
//            20, 0);
//    writeln(app.data);
//}
+/