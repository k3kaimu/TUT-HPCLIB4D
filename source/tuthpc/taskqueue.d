module tuthpc.taskqueue;

import tuthpc.hosts;
import tuthpc.constant;

import std.range;


void makeQueueScript(R)(ref R orange, Cluster cluster, string[][] prescripts, string binName, string[][] postscripts,uint nodes, uint ppn = 0)
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
    .put(orange, ["mpirun -np $MPI_PROCS ", binName, "\n"]);
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

    .put(orange, "#!/bin/sh\n");
    orange.formattedWrite("#PBS -l nodes=%s:ppn=%s\n", nodes, ppn);
    orange.formattedWrite("#PBS -q %s\n", clusters[cluster].queueName);
    .put(orange, "#MPI_PROCS=`wc -l $PBS_NODEFILE | awk '{print $1}'`\n");
    .put(orange, "cd $PBS_O_WORKDIR\n");
}


void jobRun(uint nodes, uint ppn, void delegate() dg,
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

        makeQueueScript(app, cluster,
                        [["JOB_ENV_TUTHPC_FILE = %s".format(file)],
                         ["JOB_ENV_TUTHPC_LINE = %s".format(line)]],
                        name, [], nodes, ppn);

        import std.file;
        std.file.write("pushToQueue.sh", app.data);

        auto qsub = execute(["qsub", "pushToQueue.sh"]);
        writeln(qsub.status == 0 ? "Successed push to queue" : "Failed push to queue");
        writeln("qsub output: ", qsub.output);
    }else{
        auto envs = environment.toAA();
        enforce("JOB_ENV_TUTHPC_LINE" in envs && "JOB_ENV_TUTHPC_FILE" in envs, "cannot file environments: 'JOB_ENV_TUTHPC_LINE' and 'JOB_ENV_TUTHPC_FILE'");

        if(envs["JOB_ENV_TUTHPC_FILE"] == file && envs["JOB_ENV_TUTHPC_LINE"].to!size_t == line){
            dg();
        }
    }
}

unittest 
{
    import std.stdio;
    auto app = appender!string();
    makeQueueScript(app, Cluster.wdev,
            [["aaaaa", "bbbbb"], ["cccc", "ddddd"]],
            "bash",
            [["aaaaa", "bbbbb"], ["cccc", "ddddd"]],
            20, 0);
    writeln(app.data);
}