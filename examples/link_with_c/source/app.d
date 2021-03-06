import tuthpc.taskqueue;

extern(C) void func_A(float a, float b, int c);

void main()
{
    auto env = defaultJobEnvironment;
    auto taskList = new MultiTaskList!void();

    foreach(i; 0 .. 10)
        taskList.append(&func_A, 1.0, 2.0, i);

    run(taskList, env);
}
