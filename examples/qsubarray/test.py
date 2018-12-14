# ./qsubarray python test.py

def submitJob(taskList):
    # qsubarrayにタスクの数を伝える
    print("TUTHPCLIB4D:submit:{0}".format(len(taskList)))

    # qsubarrayからこのプロセスが実行すべきタスクのインデックスを得る
    index = int(input())

    # 該当するindexのものを実行
    if index >= 0 and index < len(taskList):
        taskList[index]();


taskList = []

def makeTask(i):
    def task():
        print("Hello, world!: {0}".format(i))

    return task

for i in range(10):
    taskList.append(makeTask(i))

submitJob(taskList)


def makeTask2(i):
    def task():
        print("This is the 2nd job: {0}".format(i))

    return task;

taskList = []
for i in range(5):
    taskList.append(makeTask2(i))

submitJob(taskList)
