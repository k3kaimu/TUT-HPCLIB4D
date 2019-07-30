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
struct OnDiskVariable(T)
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
        if(exists(filename)) {
            _payload._value = msgpack.unpack!T(cast(ubyte[]) std.file.read(filename));
        }
    }


    ref T get()
    {
        return _payload._value;
    }


    void opAssign(T t)
    {
        _payload._value = t;
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
                auto bin = msgpack.pack(_value);
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
