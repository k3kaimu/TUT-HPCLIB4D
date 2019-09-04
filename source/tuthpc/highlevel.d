module tuthpc.highlevel;

import std.range;
import tuthpc.taskqueue;

PushResult!U qmap(T, U)(PushResult!T list, U delegate(T) dg, in JobEnvironment env = defaultJobEnvironment(), string file = __FILE__, size_t line = __LINE__)
{
    auto tasks = new MultiTaskList!U();
    foreach(i; 0 .. list.retvals.length) {
        tasks.append((size_t i){
            return dg(list.retvals[i].get);
        }, i);
    }

    return list.afterSuccessRun(tasks, env, file, line);
}


PushResult!U qmap(R, U, X)(R range, U delegate(X) dg, in JobEnvironment env = defaultJobEnvironment(), string file = __FILE__, size_t line = __LINE__)
if(isInputRange!R)
{
    auto tasks = new MultiTaskList!U();
    foreach(e; range) {
        tasks.append(dg, e);
    }

    return run(tasks, env, file, line);
}


PushResult!void qeach(T)(PushResult!T list, void delegate(T) dg, in JobEnvironment env = defaultJobEnvironment(), string file = __FILE__, size_t line = __LINE__)
{
    return qmap(list, dg, env, file, line);
}


PushResult!void qeach(R, X)(R range, void delegate(X) dg, in JobEnvironment env = defaultJobEnvironment(), string file = __FILE__, size_t line = __LINE__)
if(isInputRange!R)
{
    return qmap(list, dg, env, file, line);
}
