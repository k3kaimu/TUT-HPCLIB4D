import std.stdio;
import tuthpc.taskqueue;


void main()
{
    auto env = defaultJobEnvironment;

    {
        auto list = new MultiTaskList!void();
        foreach(i; 0 .. 16)
            foreach(j; 0 .. 16)
                list.append!writefln("Hello, TUTHPCLib4D! %s", i);

        assert(list.length == 16*16);
        run(list, env);
    }
    {
        auto list = uniqueTaskAppender!void((size_t i){ writefln("Hello, TUTHPCLib4D! %s", i); });
        foreach(i; 0 .. 16)
            foreach(j; 0 .. 16)
                list.append(i);

        assert(list.length == 16);
        run(list, env);
    }
    {
        auto list = new MultiTaskList!void();
        foreach(i; 0 .. 16)
            list.append((size_t i){
                auto list2 = new MultiTaskList!void();
                foreach(j; 0 .. 16)
                    list2.append!writefln("Hello, TUTHPCLib4D! %s:%s", i, j);

                run(list2, env);
            }, i);

        run(list, env);
    }
    {
        foreach(i; 0 .. 2){
            auto list = new MultiTaskList!void();
            list.append!writefln("Hello, TUTHPCLib4D! %s", i);
            run(list, env);
        }
    }

    import std.range : iota;
    {
        iota(16).toTasks!(i => writefln("Hello, TUTHPCLib4D! %s", i)).run(env);
    }
    {
        env.maxArraySize = 5;
        iota(16).toTasks!writeln.run(env);
    }
    {
        foreach(i; iota(16).runAsTasks(env))
            writefln("Hello, TUTHPCLib4D! %s", i);
    }
    {
        foreach(i, e; iota(16, 32).runAsTasks(env))
            writefln("Hello, TUTHPCLib4D! %s %s", i, e);
    }
    {
        auto taskList = new MultiTaskList!void();
        foreach(e; iota(16).appendAsTasks(taskList))
            writefln("Hello, TUTHPCLib4D! %s", e);

        foreach(i, e; iota(16).appendAsTasks(taskList))
            writefln("Hello, TUTHPCLib4D! %s %s", i, e);

        taskList.run(env);
    }
}
