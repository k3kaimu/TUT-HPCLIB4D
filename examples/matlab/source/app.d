import std.process;
import std.stdio;
import tuthpc.taskqueue;
import core.thread;


void main(string[] args)
{
	writeln(args);
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

			//Thread.sleep(10.minutes);
		}, i);
	
	pushArrayJob(taskList, env);
}
