module tuthpc.tasklist;

import std.exception;
import std.format;
import std.random;
import std.range;
import std.traits;


import tuthpc.variable;

enum bool isTaskList(TL) = is(typeof((TL taskList){
    foreach(i; 0 .. taskList.length){
        taskList[i]();
    }
}));


alias ReturnTypeOfTaskList(TL) = typeof(TL.init[0]());


final class MultiTaskList(T)
{
    this() {}


    this(TL)(TL taskList)
    if(isTaskList!TL)
    {
        this ~= taskList;
    }


    T delegate() opIndex(size_t idx)
    {
        return _tasks[idx];
    }


    size_t length() const @property { return _tasks.length; }


    void opOpAssign(string op : "~", TL)(TL taskList)
    if(isTaskList!TL && is(ReturnTypeOfTaskList!TL == T))
    {
        foreach(i; 0 .. taskList.length){
            this.append(function(typeof(taskList[0]) fn){ return fn(); }, taskList[i]);
        }
    }


    void opOpAssign(string op : "~")(MultiTaskList!T other)
    {
        _tasks ~= other._tasks;
    }


  private:
    T delegate()[] _tasks;
}


void append(R, F, T...)(MultiTaskList!R list, F func, T args)
{
    list._tasks ~= delegate() { return func(args); };
}


void append(alias func, R, T...)(MultiTaskList!R list, T args)
{
    static if(is(R == void))
        list._tasks ~= delegate() { func(args); };
    else
        list._tasks ~= delegate() { return func(args); };
}


unittest
{
    static assert(isTaskList!(MultiTaskList!void));

    int a = -1;
    auto taskList = new MultiTaskList!void(
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
    auto taskList = new MultiTaskList!void();
    taskList ~= iota(5).map!(i => (){ a = i; });

    assert(taskList.length == 5);
}


final class TaskAppender(R, Args...)
{
    this(R delegate(Args) dg)
    {
        _dg = dg;
    }


    this(R function(Args) fp)
    {
        import std.functional : toDelegate;

        _dg = toDelegate(fp);
    }


    void append(Args args)
    {
        _list ~= ArgsType(args);
    }


    alias put = append;


    auto opIndex(size_t idx)
    {
        Caller dst = {_dg, _list[idx]};
        return dst;
    }


    size_t length() const @property { return _list.length; }


  private:
    R delegate(Args) _dg;
    ArgsType[] _list;

    static struct ArgsType { Args args; }
    static struct Caller
    {
        R delegate(Args) _dg;
        ArgsType _args;

        R opCall(){ return _dg(_args.args); }
    }
}


auto taskAppender(R, Args...)(R delegate(Args) dg) { return new TaskAppender!(R, Args)(dg); }


auto taskAppender(R, Args...)(R function(Args) fp) { return new TaskAppender!(R, Args)(fp); }

unittest
{
    import std.range;

    int[][int] arrAA;

    auto app = taskAppender!void((int a){ arrAA[a] ~= a; });

    std.range.put(app, iota(10).chain(iota(10)));
    assert(app.length == 20);
    foreach(i; 0 .. app.length)
        app[i]();

    assert(arrAA.length == 10);
    foreach(k, e; arrAA)
        assert(e.length == 2);
}


final class UniqueTaskAppender(R, Args...)
{
    this(R delegate(Args) dg)
    {
        _dg = dg;
    }


    this(R function(Args) fp)
    {
        import std.functional : toDelegate;

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
    R delegate(Args) _dg;
    ArgsType[] _list;
    bool[ArgsType] _set;

    static struct ArgsType { Args args; }
    static struct Caller
    {
        R delegate(Args) _dg;
        ArgsType _args;

        R opCall(){ return _dg(_args.args); }
    }
}


auto uniqueTaskAppender(R, Args...)(R delegate(Args) dg) { return new UniqueTaskAppender!(R, Args)(dg); }


auto uniqueTaskAppender(R, Args...)(R function(Args) fp) { return new UniqueTaskAppender!(R, Args)(fp); }


unittest
{
    import std.range;

    int[][int] arrAA;

    auto app = uniqueTaskAppender!void((int a){ arrAA[a] ~= a; });

    std.range.put(app, iota(10).chain(iota(10)));
    assert(app.length == 10);
    foreach(i; 0 .. app.length)
        app[i]();

    foreach(k, e; arrAA)
        assert(e.length == 1);
}


unittest
{
    import std.range;

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

    auto app = uniqueTaskAppender!void(function(ComplexData d){  });
    std.range.put(app, args);

    assert(app.length == 1);
}



void taskShuffle(T, Rnd)(ref MultiTaskList!T taskList, ref Rnd rnd)
if(isUniformRNG!Rnd)
{
    taskList._tasks.randomShuffle(rnd);
}



class ResumableTask(TL)
if(isTaskList!TL)
{
    this(TL taskList, string filename, size_t saveThrottle)
    {
        _taskList = taskList;
        _throttle = saveThrottle;
        _isFetched = false;
        if(_throttle == 0)
            _throttle = 1;

      static if(is(ReturnTypeOfTaskList!TL == void))
      {
        _dones = OnDiskVariable!size_t(filename);
      }
      else
      {
        _rets = OnDiskVariable!(ReturnTypeOfTaskList!TL[])(filename);
      }
    }


    size_t numOfTotal()
    {
        return _taskList.length;
    }


    bool isDone()
    {
        return this.numOfDone == this.numOfTotal;
    }


    size_t numOfDone()
    {
        if(!_isFetched) this.fetch();

        static if(is(ReturnTypeOfTaskList!TL == void))
            return _dones.isNull ? 0 : _dones.get;
        else
            return _rets.isNull ? 0 : _rets.get.length;
    }


    void opCall()
    {
        this.fetch();

        foreach(i; this.numOfDone() .. _taskList.length) {
            static if(is(ReturnTypeOfTaskList!TL == void))
            {
                _taskList[i]();
                _dones = i + 1;
                if(i % _throttle == 0)
                    _dones.flush();
            }
            else
            {
                auto v = _taskList[i]();
                _rets = _rets.get ~ v;
                if(i % _throttle == 0)
                    _rets.flush();
            }
        }

        this.flush();
    }


    void fetch()
    {
        static if(is(ReturnTypeOfTaskList!TL == void))
            _dones.fetch();
        else
            _rets.fetch();

        _isFetched = true;
    }


    void flush()
    {
        static if(is(ReturnTypeOfTaskList!TL == void))
            _dones.flush();
        else
            _rets.flush();
    }


    void nullify()
    {
        _isFetched = false;
        static if(is(ReturnTypeOfTaskList!TL == void))
            _dones.nullify();
        else
            _rets.nullify();
    }


  static if(!is(ReturnTypeOfTaskList!TL == void))
  {
    ReturnTypeOfTaskList!TL[] returns()
    {
        this.fetch();
        return _rets.isNull ? [] : _rets.get;
    }
  }


  private:
    TL _taskList;
    ptrdiff_t _throttle;
    bool _isFetched;

  static if(is(ReturnTypeOfTaskList!TL == void))
    OnDiskVariable!size_t _dones;
  else
  {
    OnDiskVariable!(ReturnTypeOfTaskList!TL[]) _rets;
  }
}


auto toResumable(TL)(TL taskList, string filename, size_t throttle = size_t.max)
{
    return new ResumableTask!TL(taskList, filename, throttle);
}

unittest
{
    import std.file;

    string filename = "remove_this_file.bin";
    scope(exit)
        if(exists(filename))
            std.file.remove(filename);

    bool throwEx = true;
    int a = 0, b = 0;

    int delegate()[] tasks = [
        (){ a = 1; b = 1; return 1; },
        (){ a = 2; if(throwEx) throw new Exception(""); return 2;  },
        (){ a = 3; return 3; }
    ];

    auto list = toResumable(tasks, filename);

    assert(list.numOfTotal == 3);
    assert(list.numOfDone == 0);
    assertThrown(list());
    assert(list.numOfDone == 1);
    assert(a == 2 && b == 1);

    a = 0;
    b = 0;
    throwEx = false;
    assertNotThrown(list());
    assert(list.numOfDone == 3);
    assert(a == 3 && b == 0);
    assert(list.returns == [1, 2, 3]);
}

unittest
{
    import std.file;

    string filename = "remove_this_file.bin";
    scope(exit)
        if(exists(filename))
            std.file.remove(filename);

    bool throwEx = true;
    int a = 0, b = 0;

    int delegate()[] tasks = [
        (){ a = 1; b = 1; return 1; },
        (){ a = 2; if(throwEx) throw new Exception(""); return 2;  },
        (){ a = 3; return 3; }
    ];

    auto list = toResumable(tasks, filename);

    assert(list.numOfTotal == 3);
    assert(list.numOfDone == 0);
    assertThrown(list());
    assert(list.numOfDone == 1);
    assert(a == 2 && b == 1);
    destroy(list);

    auto list2 = toResumable(tasks, filename);
    assert(list2.numOfDone == 1);

    a = 0;
    b = 0;
    throwEx = false;
    assertNotThrown(list2());
    assert(list2.numOfDone == 3);
    assert(a == 3 && b == 0);
    assert(list2.returns == [1, 2, 3]);
}



class PartialTaskList(TL, R)
if(isTaskList!TL && isRandomAccessRange!R && hasLength!R)
{
    this(TL taskList, R indecies)
    {
        _taskList = taskList;
        _indecies = indecies;
    }


    size_t length() { return _indecies.length; }


    auto opIndex(size_t i) { return _taskList[_indecies[i]]; }


  private:
    TL _taskList;
    R _indecies;
}


auto toPartial(TL, R)(TL taskList, R indecies)
if(isTaskList!TL && isRandomAccessRange!R && hasLength!R)
{
    return new PartialTaskList!(TL, R)(taskList, indecies);
}



class SplitMergeResumableTasks(TL)
{
    this(TL taskList, size_t numOfDiv, string filename, size_t throttle)
    {
        foreach(i; 0 .. numOfDiv) {
            size_t startIndex = taskList.length / numOfDiv * i;
            size_t endIndex = taskList.length / numOfDiv * (i+1);
            if(i == numOfDiv - 1)
                endIndex = taskList.length;

            _list ~= makePartialTask(taskList, startIndex, endIndex, "%s_%s".format(filename, i), throttle);
        }
    }


    size_t length()
    {
        return _list.length;
    }


    auto opIndex(size_t i)
    {
        return _list[i];
    }


    auto returns()
    {
        ReturnTypeOfTaskList!TL[] rets;
        foreach(i; 0 .. _list.length)
            rets ~= _list[i].returns;

        return rets;
    }


    void nullify()
    {
        foreach(ref e; _list)
            e.nullify();
    }


    MultiTaskList!void toMultiTaskList()
    {
        auto list = new MultiTaskList!void();
        foreach(i; 0 .. _list.length) {
            list.append!((i, ts) => ts[i]())(i, _list);
        }

        return list;
    }


  private:
    ReturnType!makePartialTask[] _list;

    static
    auto makePartialTask(TL taskList, size_t i, size_t j, string filename, size_t throttle)
    {
        return toResumable(toPartial(taskList, iota(i, j)), filename, throttle);
    }
}


auto toSplitMergeResumable(TL)(TL taskList, size_t nDivs, string filename, size_t throttle = size_t.max)
{
    return new SplitMergeResumableTasks!TL(taskList, nDivs, filename, throttle);
}

unittest
{
    import std.file;

    string filename = "remove_this_file";
    auto rmtestfiles() {
        foreach(i; 0 .. 2) {
            auto fn = "%s_%s".format(filename, i);
            if(exists(fn)) {
                std.file.remove(fn);
            }
        }
    }

    rmtestfiles();

    scope(exit) {
        rmtestfiles();
    }

    bool throwEx = true;
    int a = 0, b = 0;

    int delegate()[] tasks = [
        (){ a = 1; b = 1; return 1; },
        (){ a = 2; if(throwEx) throw new Exception(""); return 2;  },
        (){ a = 3; return 3; }
    ];

    auto list = toSplitMergeResumable(tasks, 2, filename);
    assert(list.toMultiTaskList.length == 2);
    assertNotThrown(list[0]());
    assert(a == 1 && b == 1);
    b = 0;
    assertThrown(list[1]());
    assert(a == 2);
    destroy(list);

    auto list2 = toSplitMergeResumable(tasks, 2, filename);

    throwEx = false;
    list2[1]();
    assert(a == 3);
    assert(list2.returns == [1, 2, 3]);
    destroy(list2);
}

// unittest
// {
//     import std.file;

//     string filename = "remove_this_file";
//     scope(exit)
//         foreach(i; 0 .. 2) {
//             auto fn = "%s_%s".format(filename, i);
//             if(exists(fn))
//                 std.file.remove(fn);
//         }

//     bool throwEx = true;
//     int a = 0, b = 0;

//     int delegate()[] tasks = [
//         (){ a = 1; b = 1; return 1; },
//         (){ a = 2; if(throwEx) throw new Exception(""); return 2;  },
//         (){ a = 3; return 3; }
//     ];

//     auto list = toSplitMergeResumable(tasks, 2, filename);
//     assertNotThrown(list[0]());
//     assert(a == 1 && b == 1);
//     b = 0;
//     assertThrown(list[1]());
//     assert(a == 2);

//     throwEx = false;
//     list[1]();
//     assert(a == 3);
// }