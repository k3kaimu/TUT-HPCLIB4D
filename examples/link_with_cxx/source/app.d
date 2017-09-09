import tuthpc.taskqueue;
import core.thread;
import core.time;
extern(C++) void func_A(float a, float b, int c);


void func_job(float a, float b, int c)
{
    func_A(a, b, c);
    Thread.sleep(1.minutes);
}

void main()
{
    JobEnvironment env;
    env.queueName = "wEduq";

    auto taskList = new MultiTaskList();

    foreach(i; 0 .. 100)
        taskList.append(&func_job, 1.0, 2.0, i);

    run(taskList, env);
}
