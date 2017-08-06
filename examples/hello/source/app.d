import std.stdio;
import tuthpc.taskqueue;


void main()
{
    JobEnvironment env;

    {
        auto list = new MultiTaskList();
        foreach(i; 0 .. 16)
            foreach(j; 0 .. 16)
                list.append((size_t i){
                    writefln("Hello, TUTHPCLib4D! %s", i);
                }, i);

        assert(list.length == 16*16);
        tuthpc.taskqueue.run(list, env);
    }
    {
        auto list = uniqueTaskAppender((size_t i){ writefln("Hello, TUTHPCLib4D! %s", i); });
        foreach(i; 0 .. 16)
            foreach(j; 0 .. 16)
                list.append(i);

        assert(list.length == 16);
        tuthpc.taskqueue.run(list, env);
    }
    {
        auto list = new MultiTaskList();
        foreach(i; 0 .. 16)
            list.append((size_t i){
                auto list2 = new MultiTaskList();
                foreach(j; 0 .. 16)
                    list2.append((size_t i, size_t j){
                        writefln("Hello, TUTHPCLib4D! %s:%s", i, j);
                    }, i, j);

                tuthpc.taskqueue.run(list2, env);
            }, i);

        tuthpc.taskqueue.run(list, env);
    }
    {
        foreach(i; 0 .. 16){
            auto list = new MultiTaskList();
            list.append((size_t i){
                    writefln("Hello, TUTHPCLib4D! %s", i);
            }, i);
            tuthpc.taskqueue.run(list, env);
        }
    }
}
