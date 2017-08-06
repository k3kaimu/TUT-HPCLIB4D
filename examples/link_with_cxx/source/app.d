import tuthpc.taskqueue;

extern(C++) void func_A(float a, float b, int c);

void main()
{
    JobEnvironment env;
    auto taskList = new MultiTaskList();

    foreach(i; 0 .. 10)
        taskList.append(&func_A, 1.0, 2.0, i);

    run(taskList, env);
}
