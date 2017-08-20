#include <iostream>
#include "tuthpc.hpp"


extern "C"
void tuthpc_main(int argc, char ** argv)
{
    // タスクリスト
    tuthpc::TaskList taskList;

    for(int i = 0; i < 10; ++i){
        // 変数はコピーキャプチャしたほうが無難
        // 特に，ループ変数はかならずコピーキャプチャすること
        taskList.push_back([i](){
            std::cout << "Hello, World!: "
                      << i
                      << std::endl;
        });
    }

    // タスクの実行
    tuthpc::runTasks(taskList);
}
