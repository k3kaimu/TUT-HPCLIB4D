import std.process;
import tuthpc.taskqueue;

void main()
{
	mainJob();
}

void mainJob()
{
	JobEnvironment env;
	env.loadModules ~= "matlab";

	auto taskList = new MultiTaskList();
	foreach(i; 0 .. 20)
		taskList.append((size_t i){
			auto pipes = pipeProcess(["matlab", "-nodisplay"], Redirect.stdin);
			scope(exit) wait(pipes.pid);

			pipes.stdin.writefln("magic(%s)", i);
			pipes.stdin.flush();
			pipes.stdin.close();
		}, i);
	
	pushArrayJob(taskList, env);
}
