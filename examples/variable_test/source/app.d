import std.algorithm;
import std.conv;
import std.format;
import std.range;
import std.stdio;
import std.typecons;
import tuthpc.taskqueue;
import tuthpc.variable;


/**
# when you want to submit job
./variable_test

# when you want to show results
./variable_test --th:show
*/
void main()
{
    auto env = defaultJobEnvironment();

    auto firstTasks = new MultiTaskList!int();

    foreach(i; 0 .. 10) {
        firstTasks.append((size_t i) {
            return cast(int) i^^2;
        }, i);
    }

    // ジョブ投入 & 実行
    auto res = firstTasks.run(env);


    auto secondTasks = new MultiTaskList!void();
    foreach(i; 0 .. 10) {
        secondTasks.append((size_t i) {
            writeln(res.retvals[i].path);
            writeln(res.retvals[i]);
        }, i);
    }

    // 最初のジョブの後に実行
    res.afterSuccessRun(secondTasks);

    writeln(res);
    foreach(i; 0 .. 10) {
        writeln(res.retvals[i].path);
    }
}