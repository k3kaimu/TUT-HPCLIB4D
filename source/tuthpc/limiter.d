module tuthpc.limiter;

import std.algorithm;
import std.array;
import std.conv;
import std.exception;
import std.process;
import std.range;
import std.string;


int countOfEnqueuedJobs()
{
    try{
        auto qstat = executeShell("qstat -q");
        if(qstat.status != 0) return -1;

        return qstat.output.chomp.split("\n")[$-1].split(" ")[$-1].to!uint;
    }catch(Exception ex){
        return -1;
    }
}


auto pgrepByUser()
{
    auto uname = environment.get("USER", null).enforce("USER is null");
    auto result = execute(["pgrep", "-u", uname]);
    enforce(result.status == 0, "pgrep is failed");

    return result.output.splitter("\n").filter!"a.length".array();
}


size_t numOfProcessOfUser()
{
    return pgrepByUser.walkLength();
}
