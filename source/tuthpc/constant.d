module tuthpc.constant;


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
}

shared immutable ClusterInfo[Cluster] clusters; 

shared static this()
{
	clusters =  [
		Cluster.cdev:
			ClusterInfo("cdev", 30, 16, 48, "cdev", "rchq"),
		Cluster.wdev:
			ClusterInfo("wdev", 30, 20, 100, "wdev", "wLrchq"),
	];
}
