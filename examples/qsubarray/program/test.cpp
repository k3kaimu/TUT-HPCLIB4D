// Comple: g++ -std=c++14 test.cpp
// Usage: qsubarray ./a.out
#include <functional>
#include <iostream>
#include <vector>

using TaskList = std::vector<std::function<void()>>;

void submitJob(TaskList & taskList)
{
    std::cout << "TUTHPCLIB4D:submit:" << taskList.size() << std::endl;

    int idx;
    std::cin >> idx;

    if(idx >= 0 && idx < taskList.size())
        taskList[idx]();
}


int main(void)
{
    TaskList list;
    for(int i = 0; i < 10; ++i)
        list.push_back([=](){ std::cout << i << std::endl; });

    submitJob(list);
}
