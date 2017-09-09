module tuthpc.tasklist;


enum bool isTaskList(TL) = is(typeof((TL taskList){
    foreach(i; 0 .. taskList.length){
        taskList[i]();
    }
}));


final class MultiTaskList
{
    this() {}


    this(TL)(TL taskList)
    if(isTaskList!TL)
    {
        this ~= taskList;
    }


    void delegate() opIndex(size_t idx)
    {
        return _tasks[idx];
    }


    size_t length() const @property { return _tasks.length; }


    void opOpAssign(string op : "~", TL)(TL taskList)
    if(isTaskList!TL)
    {
        foreach(i; 0 .. taskList.length){
            this.append(function(typeof(taskList[0]) fn){ fn(); }, taskList[i]);
        }
    }


  private:
    void delegate()[] _tasks;
}


void append(F, T...)(MultiTaskList list, F func, T args)
{
    list._tasks ~= delegate() { func(args); };
}


void append(alias func, T...)(MultiTaskList list, T args)
{
    list._tasks ~= delegate() { func(args); };
}


unittest
{
    static assert(isTaskList!MultiTaskList);

    int a = -1;
    auto taskList = new MultiTaskList(
        [
            { a = 0; },
            { a = 1; },
            { a = 2; },
            { a = 3; },
        ]);

    assert(taskList.length == 4);
    taskList[0]();
    assert(a == 0);
    taskList[1]();
    assert(a == 1);
    taskList[2]();
    assert(a == 2);
    taskList[3]();
    assert(a == 3);

    taskList.append((int b){ a = b; }, 4);
    assert(taskList.length == 5);
    taskList[4]();
    assert(a == 4);

    taskList.append!(b => a = b)(5);
    assert(taskList.length == 6);
    taskList[5]();
    assert(a == 5);
}

unittest
{
    import std.algorithm;
    import std.range;

    int a;
    auto taskList = new MultiTaskList();
    taskList ~= iota(5).map!(i => (){ a = i; });

    assert(taskList.length == 5);
}


final class UniqueTaskAppender(Args...)
{
    this(void delegate(Args) dg)
    {
        _dg = dg;
    }


    this(void function(Args) fp)
    {
        _dg = toDelegate(fp);
    }


    void append(Args args)
    {
        ArgsType a = ArgsType(args);
        if(a !in _set){
            _list ~= a;
            _set[a] = true;
        }
    }


    alias put = append;


    auto opIndex(size_t idx)
    {
        Caller dst = {_dg, _list[idx]};
        return dst;
    }


    size_t length() const @property { return _list.length; }


  private:
    void delegate(Args) _dg;
    ArgsType[] _list;
    bool[ArgsType] _set;

    static struct ArgsType { Args args; }
    static struct Caller
    {
        void delegate(Args) _dg;
        ArgsType _args;

        void opCall(){ _dg(_args.args); }
    }
}


auto uniqueTaskAppender(Args...)(void delegate(Args) dg) { return new UniqueTaskAppender!Args(dg); }


auto uniqueTaskAppender(Args...)(void function(Args) fp) { return new UniqueTaskAppender!Args(fp); }


unittest
{
    int[][int] arrAA;

    auto app = uniqueTaskAppender((int a){ arrAA[a] ~= a; });

    .put(app, iota(10).chain(iota(10)));
    assert(app.length == 10);
    foreach(i; 0 .. app.length)
        app[i]();

    foreach(k, e; arrAA)
        assert(e.length == 1);
}


unittest
{
    struct S { int f; }
    struct ComplexData
    {
        int b;
        long c;
        string d;
        int[] e;
        S s;
    }

    ComplexData[] args;
    foreach(i; 0 .. 10){
        ComplexData data;
        data.b = 1;
        data.c = 2;
        data.d = "foo";
        data.e = new int[3];
        data.s.f = 3;
        args ~= data;
    }

    auto app = uniqueTaskAppender(function(ComplexData d){  });
    .put(app, args);

    assert(app.length == 1);
}