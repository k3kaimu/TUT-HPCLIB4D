import tuthpc.highlevel;

import std;

void main()
{
    iota(10)
    .qmap((int i){ return i^^2; })
    .qmap((int i){ return i.to!string ~ "foobar"; })
    .qeach((string s) { writefln!"Hello: %s"(s); });
}
