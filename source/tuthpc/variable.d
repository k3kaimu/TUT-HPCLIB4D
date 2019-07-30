module tuthpc.variable;

import std.file;
import msgpack;


private
enum bool isMessagePackable(T) = is(typeof((T t){
    ubyte[] binary = msgpack.pack(t);
    // T u = msgpack.unpack!T(binary);
}));

unittest
{
    static assert(isMessagePackable!int);
}



/**
ディスク上に変数の内容を書き出し保存します．
また，保存された内容を読み出せます．
書き出しや読み出しのためのシリアライゼーションとしてmsgpackを利用しています．
*/
struct OnDiskVariable(T)
if(isMessagePackable!T)
{
    /**
    保存する先のファイル名を指定します．
    */
    this(string filename)
    {
        _filename = filename;
    }


    /**
    ファイルとして保存されている内容を読み出します．
    */
    T get()
    {
        auto bin = cast(ubyte[])std.file.read(_filename);
        return msgpack.unpack!T(bin);
    }


    /**
    ファイルに変数の内容を書き出します．
    */
    void opAssign(T t)
    {
        auto bin = msgpack.pack(t);
        std.file.write(_filename, bin);
    }


    string _filename;
}


unittest
{
    string filename = "remove_this_file.bin";

    auto va = OnDiskVariable!int(filename);

    va = 12;
    assert(va.get == 12);

    va = 1;
    assert(va.get == 1);

    auto vb = OnDiskVariable!int(filename);

    vb = 2;
    assert(va.get == 2);
    assert(vb.get == 2);

    scope(exit)
        std.file.remove(filename);
}
