

taskList = []

def func(i):
  def task():
    print("Hello, world! :{0}".format(i))
  return task

for i in range(10):
	taskList.append(func(i))

print("TUTHPCLIB4D:submit:{0}".format(len(taskList)))

index = int(input())

if index >= 0 and index < len(taskList):
	taskList[index]();

