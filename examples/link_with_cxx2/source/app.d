import tuthpc.taskqueue;
import std.range;
import core.runtime;

extern(C)
{
    void tuthpc_main(int argc, char** argv);

    alias TaskCallback = void function(void* obj, uint i);

    void tuthpc_run_tasks(void* obj, uint size, TaskCallback callback)
    {
        foreach(i; iota(size).runAsTasks)
            callback(obj, i);
    }
}


void main()
{
    tuthpc_main(Runtime.cArgs.tupleof);
}
