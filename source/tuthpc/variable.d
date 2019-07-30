module tuthpc.variable;

import std.algorithm : move;
import std.file;
import std.typecons;
import msgpack;


private
enum bool isMessagePackable(T) = is(typeof((T t){
    ubyte[] binary = msgpack.pack(t);
    T u = msgpack.unpack!T(binary);
}));

unittest
{
    static assert(isMessagePackable!int);
}



/**
ディスク上に変数の内容を書き出し保存します．
また，保存された内容を読み出せます．

書き出しや読み出しのためのシリアライゼーションとしてmsgpackを利用しています．
ファイルからの読み出しはインスタンス作成時のみ実行されます．
また，ファイルへの書き出しはインスタンスの破棄時のみ実行されます．
*/
struct OnDiskVariable(T, Flag!"withFieldName" withFieldName = Yes.withFieldName)
if(isMessagePackable!T)
{
    /**
    保存する先のファイル名を指定し，インスタンスを作成します．
    */
    this(string filename, Flag!"isReadOnly" isReadOnly = No.isReadOnly)
    {
        _payload = refCounted(Payload.init);

        _payload._filename = filename;
        _payload._isReadOnly = isReadOnly;
        if(exists(filename))
            fetch();
    }


    ref T get()
    {
        return _payload._value;
    }


    void opAssign(T t)
    {
        _payload._value = t;
    }


    void fetch()
    {
        _payload.fetch();
    }


    void flush()
    {
        _payload.flush();
    }


  private:
    RefCounted!Payload _payload;


    static struct Payload
    {
        string _filename;
        bool _isReadOnly;
        T _value;

        ~this()
        {
            if(_filename !is null && !_isReadOnly) {
                this.flush();
            }
        }


        void fetch()
        {
            msgpack.unpack!withFieldName(cast(ubyte[]) std.file.read(_filename), _value);
        }


        void flush()
        {
            auto bin = msgpack.pack!withFieldName(_value);
            std.file.write(_filename, bin);
        }
    }
}
}

//
unittest
{
    string filename = "remove_this_file.bin";

    auto va = OnDiskVariable!int(filename);
    scope(exit)
        std.file.remove(filename);

    va = 1;
    assert(va.get == 1);

    va = 2;
    assert(va.get == 2);
    destroy(va);

    auto vb = OnDiskVariable!int(filename, Yes.isReadOnly);
    assert(vb.get == 2);

    vb = 3;
    destroy(vb);

    auto vc = OnDiskVariable!int(filename);
    assert(vc.get == 2);

    vc = 3;
    auto vd = vc;
    destroy(vc);
    assert(vd.get == 3);

    auto ve = OnDiskVariable!int(filename, Yes.isReadOnly);
    assert(ve.get == 2);
    destroy(ve);

    destroy(vd);

    auto vf = OnDiskVariable!int(filename, Yes.isReadOnly);
    assert(vf.get == 3);
}

unittest
{
    string filename = "remove_this_file.bin";
    scope(exit) {
        assert(exists(filename));
        std.file.remove(filename);
    }

    static struct MyData
    {
        int[] array;
        string[] names;
    }

    MyData data;
    data.array = [1, 2, 3];
    data.names = ["AAA", "BBB"];

    auto va = OnDiskVariable!MyData(filename);
    va = data;

    assert(!exists(filename));
    va.flush();
    assert(exists(filename));

    auto vb = OnDiskVariable!MyData(filename);
    vb.array ~= 4;
    vb.flush();

    va.fetch();
    assert(va.get.array == [1, 2, 3, 4]);
}
