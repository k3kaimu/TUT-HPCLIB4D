module tuthpc.constant;

import std.algorithm;

enum Cluster
{
    cdev, wdev
}


struct ClusterInfo
{
    string name;
    uint maxNode;
    uint maxPPN;
    uint maxMem;
    string devHost;
    string queueName;
    bool delegate(string) isNode;
}

shared immutable ClusterInfo[Cluster] clusters; 

shared static this()
{
    clusters =  [
        Cluster.cdev:
            ClusterInfo("cdev", 30, 16, 48, "cdev", "rchq", (string hname) => hname.startsWith("csnd")),
        Cluster.wdev:
            ClusterInfo("wdev", 30, 20, 100, "wdev", "wLrchq", (string hname) => hname.startsWith("wsnd")),
    ];
}


Cluster currCluster()
{
    return Cluster.wdev;
}
