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

    bool isDevHost();
    bool isCompNode();

    static
    ClusterInfo currInstance()
    {
        immutable string envkey = "TUTHPC_CLUSTER_NAME";
        enforce(envkey in environment , "cannot find environment variable '%s'".format(envkey));

        immutable string envval = environment[envkey];

        if(envval.startsWith("TUTW"))
            return new TUTWInfo();
        else if(envval.startsWith("KyotoB"))
            return new KyotoBInfo();
        else if(envval.startsWith("LocalPC"))
            return null;
        else
            enforce(0, "The value of '%s' must be 'LocalPC' or 'TUTW' or 'KyotoB'.");

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


class KyotoBInfo : ClusterInfo
{
  override @property
  {
    string name() { return "KyotoB"; }
    uint maxNode() { return 26; }
    uint maxPPN() { return 72; }
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
        import std.socket;
        return Socket.hostName.startsWith("laurel");
    }

    bool isCompNode()
    {
        import std.socket;
        return Socket.hostName.startsWith("nb-");
    }
  }
}
