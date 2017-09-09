#include <iostream>
#include <cstdlib>

void func_A(float a, float b, int c)
{
    if(c == 4){
        exit(1);
    }

    std::cout << a << ", " << b << ", " << c << std::endl;
}
