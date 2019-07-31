import tuthpc.variable;
import tuthpc.taskqueue;
import std.stdio;
import std.format;
import std.typecons;


/**
# when you want to submit job
./variable_test

# when you want to show results
./variable_test --th:show
*/
void main()
{
    auto env = defaultJobEnvironment();

    auto taskList = new MultiTaskList!int();

    foreach(i; 0 .. 10) {
        taskList.append((size_t i) {
            return cast(int) i^^2;
        }, i);
    }

    // ジョブ投入 & 実行
    auto res = taskList.run(env);

    // ジョブの実行結果の表示
    if(env.isShowMode) {
        foreach(i, e; res.retvals){
            if(e.isNull)
                writefln!"%s: null"(i);
            else
                writefln!"%s: %s"(i, e.get);
        }
    }
}