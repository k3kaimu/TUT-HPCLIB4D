module tuthpc.taskqueue;

import tuthpc.hosts;
import tuthpc.constant;

import std.range;
import std.format;


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


void jobRun(MultiTaskList taskList, uint nodes = 1, uint ppn = 1, string file = __FILE__, size_t line = __LINE__)
{
    if(nowRunningOnClusterDevelopmentHost() || nowRunningOnClusterComputingNode()){
        foreach(i; iota(taskList.length))
            .jobRun(nodes, ppn, i, taskList[i], file, line);
    }else{
        import std.parallelism;

        foreach(i; iota(taskList.length).parallel)
            .jobRun(nodes, ppn, i, taskList[i], file, line);
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