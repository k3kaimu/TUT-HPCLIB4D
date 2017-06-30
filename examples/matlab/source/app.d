import std.process;
import std.stdio;
import tuthpc.taskqueue;
import core.thread;

/**
エラー時のメール受取をテストしたい場合はtrueにする
*/
enum bool bCheckErrorMail = true;

void main(string[] args)
{
	writeln(args);
	mainJob();
}

void mainJob()
{
	JobEnvironment env;
	env.loadModules ~= "matlab";
	env.queueName = "wEduq";

	if(bCheckErrorMail) {
		env.isEnabledEmailByMailgun = true;
		env.isEnabledEmailOnError = true;
	}

	auto taskList = new MultiTaskList();
	foreach(i; 0 .. 20)
		taskList.append((size_t i){
			if(i == 10 && bCheckErrorMail) throw new Exception("aaaaa");
			auto pipes = pipeProcess(["matlab", "-nodisplay"], Redirect.stdin);
			scope(exit) wait(pipes.pid);

			pipes.stdin.writefln("magic(%s)", i);
			pipes.stdin.flush();
			pipes.stdin.close();
		}, i);
	
	pushArrayJob(taskList, env);
}
