module tuthpc.hosts;

import tuthpc.constant;

import std.algorithm;
import std.functional;

immutable clusterDevelopmentHosts = ["cdev", "wdev"];


private
bool nowRunningOnClusterDevelopmentHostImpl(string myHost) pure nothrow @safe
{
    return clusterDevelopmentHosts.canFind!(reverseArgs!startsWith)(myHost);
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


bool nowRunningOnClusterDevelopmentHost()
{
    import std.socket : Socket;

    return nowRunningOnClusterDevelopmentHostImpl(Socket.hostName());
}


bool nowRunningOnCDev()
{
    return nowRunningOnClusterDevelopmentHostImpl("cdev");
}


bool nowRunningOnWDev()
{
    return nowRunningOnClusterDevelopmentHostImpl("wdev");
}


Cluster loginCluster()
in{
    assert(nowRunningOnClusterDevelopmentHost);
}
body{
    if(nowRunningOnCDev) return Cluster.cdev;
    else return Cluster.wdev;
}