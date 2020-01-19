module tuthpc.cluster;

import std.algorithm;
import std.conv;
import std.exception;
import std.format;
import std.process;


interface ClusterInfo
{
  @property:
    string name();
    uint maxNode();
    uint maxPPN();
    uint maxMemGB();
    string defaultQueueName();

    string jobID();
    uint arrayID();
    string arrayIDEnvKey();

    deprecated bool isDevHost();
    deprecated bool isCompNode();

    static
    ClusterInfo currInstance()
    {
        import tuthpc.taskqueue : EnvironmentKey;

        immutable string envkey = EnvironmentKey.CLUSTER_NAME;
        enforce(envkey in environment , "cannot find environment variable '%s'".format(envkey));

        immutable string envval = environment[envkey];

        if(envval.startsWith("TUTW"))
            return new TUTWInfo();
        else if(envval.startsWith("TUTX"))
            return new TUTXInfo();
        else if(envval.startsWith("KyotoB"))
            return new KyotoBInfo();
        else if(envval.startsWith("LocalPC"))
            return null;
        else
            enforce(0, "The value of '%s' must be 'LocalPC', 'TUTW', 'TUTX', or 'KyotoB'.");

        return null;
    }
}


class TUTWInfo : ClusterInfo
{
  override @property
  {
    string name() { return "TUTW"; }
    uint maxNode() { return 30; }
    uint maxPPN() { return 20; }
    uint maxMemGB() { return 100; }
    string defaultQueueName() { return "wLrchq"; }

    string jobID()
    {
        return environment["PBS_JOBID"];
    }

    uint arrayID()
    {
        return environment["PBS_ARRAYID"].to!uint;
    }

    string arrayIDEnvKey() { return "PBS_ARRAYID"; }

    bool isDevHost()
    {
        import std.socket;
        return Socket.hostName.startsWith("wdev");
    }

    bool isCompNode()
    {
        import std.socket;
        return Socket.hostName.startsWith("wsnd");
    }
  }
}


class TUTXInfo : ClusterInfo
{
  override @property
  {
    string name() { return "TUTX"; }
    uint maxNode() { return 14; }
    uint maxPPN() { return 28; }
    uint maxMemGB() { return 192; }
    string defaultQueueName() { return "wLrchq"; }

    string jobID()
    {
        return environment["PBS_JOBID"];
    }

    uint arrayID()
    {
        return environment["PBS_ARRAY_INDEX"].to!uint;
    }

    string arrayIDEnvKey() { return "PBS_ARRAY_INDEX"; }

    bool isDevHost()
    {
        import std.socket;
        return Socket.hostName.startsWith("xdev");
    }

    bool isCompNode()
    {
        import std.socket;
        return Socket.hostName.startsWith("xsnd");
    }
  }
}


class KyotoBInfo : ClusterInfo
{
  override @property
  {
    string name() { return "KyotoB"; }
    uint maxNode() { return 26; }
    uint maxPPN() {
        if(this.enableHTT)
            return 72;
        else
            return 36;
    }
    uint maxMemGB() { return 100; }
    string defaultQueueName() { return "gr10061b"; }

    string jobID()
    {
        return environment["PBS_JOBID"];
    }

    uint arrayID()
    {
        return environment["PBS_ARRAY_INDEX"].to!uint;
    }

    string arrayIDEnvKey()
    {
        return "PBS_ARRAY_INDEX";
    }

    bool isDevHost()
    {
        import tuthpc.taskqueue;
        import std.socket;

        bool ret1 = Socket.hostName.startsWith("laurel");
        bool ret2 = thisProcessType() == ChildProcessType.ANALYZER
                 || thisProcessType() == ChildProcessType.SUBMITTER;


        enforce(ret1 == ret2);

        return ret1;
    }

    bool isCompNode()
    {
        import tuthpc.taskqueue;
        import std.socket;
        bool ret1 = Socket.hostName.startsWith("nb-");
        bool ret2 = thisProcessType() == ChildProcessType.TASK_MANAGER
                 || thisProcessType() == ChildProcessType.TASK_PROCESSOR;

        enforce(ret1 == ret2);

        return ret1;
    }
  }


    bool enableHTT() @property
    {
        immutable string envkey = "TUTHPC_ENABLE_HTT";
        return !(envkey !in environment);
    }
}
