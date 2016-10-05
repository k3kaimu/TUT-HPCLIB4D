module tuthpc;


immutable clusterDevelopmentHosts = ["cdev", "wdev"];


private
bool nowRunningOnClusterDevelopmentHostsImpl(string myHost) pure nothrow @safe
{
    return clusterDevelopmentHosts.canFind!startsWith(myHost);
}

unittest
{
    assert(nowRunningOnClusterDevelopmentHostsImpl("cdev"));
    assert(nowRunningOnClusterDevelopmentHostsImpl("wdev"));
    assert(nowRunningOnClusterDevelopmentHostsImpl("cdev1"));
    assert(nowRunningOnClusterDevelopmentHostsImpl("wdev0"));

    assert(!nowRunningOnClusterDevelopmentHostsImpl("csnd"));
    assert(!nowRunningOnClusterDevelopmentHostsImpl("wsnd"));
    assert(!nowRunningOnClusterDevelopmentHostsImpl("csnd1"));
    assert(!nowRunningOnClusterDevelopmentHostsImpl("wsnd0"));
}


bool nowRunningOnClusterDevelopmentHosts()
{
    import std.socket : hostName;

    return nowRunningOnClusterDevelopmentHostsImpl(hostName());
}
