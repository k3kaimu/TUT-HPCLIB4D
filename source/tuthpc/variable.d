module tuthpc.variable;

import msgpack;
import std.algorithm : move;
import std.file;
import std.format;
import std.json;
import std.range;
import std.stdio : File;
import std.typecons;
import std.traits;


private
enum bool isMessagePackable(T) = is(typeof((T t){
    ubyte[] binary = msgpack.pack(t);
    T u = msgpack.unpack!T(binary);
}));

unittest
{
    static assert(isMessagePackable!int);
}

private
T getFromJSONValue(T)(ref JSONValue v)
{
    static if(is(T == JSONValue))
        return v;
    else
        return v.get!T;
}

private
enum bool isJSONable(T) = is(typeof((T t){
    JSONValue v = JSONValue(t);
    t = v.getFromJSONValue!T();
}));

unittest
{
    static assert(isJSONable!int);
    static assert(isJSONable!JSONValue);
    static assert(isJSONable!(JSONValue[]));
}



/**
ディスク上に変数の内容を書き出し保存します．
また，保存された内容を読み出せます．

書き出しや読み出しのためのシリアライゼーションとしてmsgpackかJSONを利用しています．
ファイルからの読み出しはインスタンス作成時のみ実行されます．
また，ファイルへの書き出しはインスタンスの破棄時のみ実行されます．
*/
struct OnDiskVariable(T, Flag!"withFieldName" withFieldName = Yes.withFieldName)
if(isMessagePackable!T || is(T == JSONValue) || (isArray!T && is(ElementType!T == JSONValue)))
{
    /**
    保存する先のファイル名を指定し，インスタンスを作成します．
    */
    this(string filename, Flag!"isReadOnly" isReadOnly = No.isReadOnly)
    {
        _payload = refCounted(Payload.init);

        _payload._filename = filename;
        _payload._isReadOnly = isReadOnly;
        _payload._isNull = true;
    }


    alias get this;


    string path()
    {
        return _payload._filename;
    }


    bool isNull()
    {
        return _payload._isNull;
    }


    ref T get()
    {
        return _payload._value;
    }


    void opAssign(T t)
    {
        _payload._isModified = true;
        _payload._isNull = false;
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


    void nullify()
    {
        _payload._value = T.init;
        _payload._isModified = false;
        _payload._isNull = true;
    }


    void toString(OutputRange)(ref OutputRange writer) const
    {
        if(_payload._isNull)
            .put(writer, "null");
        else
            formattedWrite(writer, "%s", _payload._value);
    }


  private:
    RefCounted!Payload _payload;


    static struct Payload
    {
        string _filename;
        bool _isReadOnly;
        T _value;
        bool _isModified;
        bool _isNull;

        ~this()
        {
            // if(_filename !is null && !_isReadOnly && _isModified && !_isNull) {
            //     this.flush();
            // }
            _isModified = false;
            _filename = null;
            _isNull = true;
        }


        void fetch(T init = T.init)
        {
            if(!exists(_filename)) {
                _value = init;
                _isNull = false;
                return;
            }

            auto buf = std.file.read(_filename);

            static if(isMessagePackable!T) {
                msgpack.unpack!withFieldName(cast(ubyte[])buf, _value);
            } else static if(isJSONable!T) {
                msgpack.Value mv = msgpack.unpack(cast(ubyte[])buf);
                JSONValue jv = msgpack.toJSONValue(mv);
                _value = jv.getFromJSONValue!T;
            } else static assert(0);

            _isNull = false;
            _isModified = false;
        }


        void flush()
        {
            if(_isReadOnly) return;

            static if(isMessagePackable!T) {
                File file = File(_filename, "w");
                auto p = packer(file.lockingBinaryWriter, withFieldName);
                p.pack(_value);
            } else static if(isJSONable!T) {
                JSONValue jv = JSONValue(_value);
                msgpack.Value mv = msgpack.fromJSONValue(jv);
                File file = File(_filename, "w");
                auto p = packer(file.lockingBinaryWriter, withFieldName);
                p.pack(mv);
            } else static assert(0);

            _isNull = false;
            _isModified = false;
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
    assert(va == 1);

    va = 2;
    va.flush();
    destroy(va);

    auto vb = OnDiskVariable!int(filename, Yes.isReadOnly);
    vb.fetch();
    assert(vb == 2);

    vb = 3;
    vb.flush();
    destroy(vb);

    auto vc = OnDiskVariable!int(filename);
    vc.fetch();
    assert(vc == 2);

    vc = 3;
    auto vd = vc;
    destroy(vc);
    assert(vd == 3);

    auto ve = OnDiskVariable!int(filename, Yes.isReadOnly);
    ve.fetch();
    assert(ve == 2);
    destroy(ve);

    vd.flush();
    destroy(vd);

    auto vf = OnDiskVariable!int(filename, Yes.isReadOnly);
    vf.fetch();
    assert(vf == 3);
}

unittest
{
    string filename = "remove_this_file.bin";
    scope(exit) {
        // assert(exists(filename));
        std.file.remove(filename);
    }

    static struct MyData
    {
        int[] array;
        string[] names;
    }

    auto va = OnDiskVariable!MyData(filename);
    va.array = [1, 2, 3];
    va.names = ["AAA", "BBB"];

    assert(!exists(filename));
    va.flush();
    assert(exists(filename));

    auto vb = OnDiskVariable!MyData(filename);
    vb.fetch();
    vb.array ~= 4;
    vb.flush();

    va.fetch();
    assert(va.array == [1, 2, 3, 4]);
}


unittest
{
    string filename = "remove_this_file.bin";
    scope(exit) {
        assert(exists(filename));
        std.file.remove(filename);
    }

    auto va = OnDiskVariable!JSONValue(filename);
    va = JSONValue(1);

    assert(!exists(filename));
    va.flush();
    assert(exists(filename));

    auto vb = OnDiskVariable!JSONValue(filename);
    vb.fetch();
    vb = JSONValue(2);
    vb.flush();

    va.fetch();
    assert(va.get.get!int == 2);
}
