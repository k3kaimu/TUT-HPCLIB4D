import std.stdio;
import std.string;
import std.conv;

alias Task = void delegate();

void submitJob(Task[] list)
{
    writefln("TUTHPCLIB4D:submit:%s", list.length);
    stdout.flush();

    long idx;
    readf!" %d"(idx);
    if(idx >= 0 && idx < list.length)
        list[idx]();
}


Task makeTask(size_t i)
{
    return (){ writeln(i); };
}


void main()
{
    Task[] list;

    foreach(i; 0 .. 10)
        list ~= makeTask(i);

    submitJob(list);
}
