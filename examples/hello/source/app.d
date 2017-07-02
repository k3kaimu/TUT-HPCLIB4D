import std.stdio;
import tuthpc.taskqueue;


void main()
{
    JobEnvironment env;

    auto list = new MultiTaskList();
    foreach(i; 0 .. 16)
        list.append((size_t i){
            writefln("Hello, TUTHPCLib4D! %s", i);
        }, i);

    tuthpc.taskqueue.run(list, env);
}
