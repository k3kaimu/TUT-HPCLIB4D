import tuthpc.variable;
import tuthpc.taskqueue;
import std.stdio;
import std.format;
import std.typecons;


void main(string[] args)
{
    bool isResultShowMode;
    if(args.length > 1 && args[1] == "show")
        isResultShowMode = true;
    else
        isResultShowMode = false;

    auto taskList = new MultiTaskList();
    OnDiskVariable!int[] results;

    foreach(i; 0 .. 10) {
        results ~= OnDiskVariable!int(
                "data%s.bin".format(i)
            );

        taskList.append((size_t i) {
            results[i] = cast(int) i^^2;
        }, i);
    }

    if(isResultShowMode) {
        foreach(i, ref e; results) {
            writefln!"%s: %s"(i, e.get);
        }
    }
    else {
        taskList.run();
    }
}