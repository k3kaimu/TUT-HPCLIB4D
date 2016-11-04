module tuthpc.hosts;

import tuthpc.constant;

import std.algorithm;
import std.functional;

immutable clusterDevelopmentHosts = ["cdev", "wdev"];


private
bool nowRunningOnClusterDevelopmentHostImpl(string myHost, in string[] clusters = clusterDevelopmentHosts) pure nothrow @safe
{
    return clusters.canFind!(reverseArgs!startsWith)(myHost);
}

unittest
{
    assert(nowRunningOnClusterDevelopmentHostImpl("cdev"));
    assert(nowRunningOnClusterDevelopmentHostImpl("wdev"));
    assert(nowRunningOnClusterDevelopmentHostImpl("cdev1"));
    assert(nowRunningOnClusterDevelopmentHostImpl("wdev0"));

    assert(!nowRunningOnClusterDevelopmentHostImpl("csnd"));
    assert(!nowRunningOnClusterDevelopmentHostImpl("wsnd"));
    assert(!nowRunningOnClusterDevelopmentHostImpl("csnd1"));
    assert(!nowRunningOnClusterDevelopmentHostImpl("wsnd0"));
}


bool nowRunningOnClusterDevelopmentHost(in string[] clusters = clusterDevelopmentHosts)
{
    import std.socket : Socket;

    return nowRunningOnClusterDevelopmentHostImpl(Socket.hostName(), clusters);
}


bool nowRunningOnClusterComputingNode()
{
    import std.socket : Socket;
    foreach(k, v; clusters)
        if(v.isNode(Socket.hostName()))
            return true;

    return false;
}


bool nowRunningOnCDev()
{
    return nowRunningOnClusterDevelopmentHost(["cdev"]);
}

unittest
{
    import std.socket;
    assert(Socket.hostName.startsWith("cdev") == nowRunningOnCDev());
}


bool nowRunningOnWDev()
{
    return nowRunningOnClusterDevelopmentHost(["wdev"]);
}

unittest
{
    import std.socket;
    assert(Socket.hostName.startsWith("wdev") == nowRunningOnWDev());
}


Cluster loginCluster()
in{
    assert(nowRunningOnClusterDevelopmentHost);
}
body{
    if(nowRunningOnCDev) return Cluster.cdev;
    else return Cluster.wdev;
}

unittest
{
    if(nowRunningOnCDev) assert(loginCluster == Cluster.cdev);
    if(nowRunningOnWDev) assert(loginCluster == Cluster.wdev);
}
