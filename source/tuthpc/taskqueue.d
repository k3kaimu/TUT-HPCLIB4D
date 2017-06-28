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
    bool isEnabledRenameExeFile = true; /// 実行ジョブファイル名を，バイナリ表現にもとづきユニークな名前に変更します．
    string originalExename;
    string renamedExename;
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
        if(jobScript is null){
            originalExename = Runtime.args[0];
            bool bStartsWithDOTSLASH = false;
            while(originalExename.startsWith("./")){
                bStartsWithDOTSLASH = true;
                originalExename = originalExename[2 .. $];
            }

            if(isEnabledRenameExeFile && renamedExename.walkLength == 0){
                import std.file;
                renamedExename = format("%s_%s", originalExename, crc32Of(cast(ubyte[])std.file.read(originalExename)).toHexString);
                writeln("Renamed file:", renamedExename);
            }

            if(!isEnabledRenameExeFile)
                jobScript = [(bStartsWithDOTSLASH ? "./" : "") ~ originalExename];
            else
                jobScript = [(bStartsWithDOTSLASH ? "./" : "") ~ renamedExename];
        }
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

    if(jenv.isEnabledRenameExeFile){
        import std.file;

        // check that the renamed file already exists
        enforce(!exists(jenv.renamedExename), "The file %s already exists. Please set a different file name or delete the file.".format(jenv.renamedExename));

        writefln("copy: %s -> %s", jenv.originalExename, jenv.renamedExename);
        std.file.copy(jenv.originalExename, jenv.renamedExename);
        enforce(execute(["chmod", "+x", jenv.renamedExename]).status == 0);
    }

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
        auto cluster = loginCluster();

        env.envs["JOB_ENV_TUTHPC_FILE"] = file;
        env.envs["JOB_ENV_TUTHPC_LINE"] = line.to!string;
        env.envs["JOB_ENV_TUTHPC_ID"] = "${PBS_ARRAYID}";

        if(env.scriptPath !is null){
            auto app = appender!string;
            makeQueueScript(app, cluster, env, taskList.length);

            import std.file;
            std.file.write("pushToQueue.sh", app.data);

            auto qsub = execute(["qsub", "pushToQueue.sh"]);
            writeln(qsub.status == 0 ? "Successed push to queue" : "Failed push to queue");
            writeln("qsub output: ", qsub.output);
        }else{
            auto pipes = pipeProcess(["qsub"], Redirect.stdin);
            scope(exit) wait(pipes.pid);
            scope(failure) kill(pipes.pid);

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
