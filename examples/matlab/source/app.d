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
	mainJob1();
}

void mainJob1()
{
	auto env = defaultJobEnvironment;
	env.loadModules ~= "matlab";
	env.queueName = "wEduq";

	if(bCheckErrorMail) {
		env.isEnabledEmailByMailgun = true;
		env.isEnabledEmailOnError = true;
	}

	auto taskList1 = new MultiTaskList();
	auto taskList2 = new MultiTaskList();
	auto taskList3 = new MultiTaskList();
	foreach(i; 0 .. 4) {
		auto dg = (size_t i){
			if(i == 10 && bCheckErrorMail) throw new Exception("aaaaa");
			auto pipes = pipeProcess(["matlab", "-nodisplay"], Redirect.stdin);
			scope(exit) wait(pipes.pid);

			pipes.stdin.writefln("magic(%s)", i);
			pipes.stdin.flush();
			pipes.stdin.close();
			Thread.sleep(1.minutes);
		};

		taskList1.append(dg, i);
		taskList2.append(dg, i);
		taskList3.append(dg, i);
	}
	
	run(taskList1, env)
	.afterExitRun(taskList2, env)
	.afterExitRun(taskList3, env);
}
