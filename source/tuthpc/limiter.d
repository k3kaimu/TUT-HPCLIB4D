module tuthpc.limiter;

import std.algorithm;
import std.array;
import std.exception;
import std.process;
import std.range;


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
