#pragma once

#include <vector>
#include <functional>


extern "C"
typedef void (*tuthpc_callback)(void * obj, unsigned int index);

extern "C"
void tuthpc_run_tasks(void * obj, unsigned int size, tuthpc_callback callback);


namespace tuthpc
{
    using Task = std::function<void()>;
    using TaskList = std::vector<Task>;


    extern "C"
    void tuthpc_task_impl(TaskList* v, int index)
    {
        if(index < v->size())
            v->at(index)();
    }


    void runTasks(TaskList& list)
    {
        tuthpc_run_tasks(&list, list.size(), (tuthpc_callback)&tuthpc_task_impl);
    }
}
