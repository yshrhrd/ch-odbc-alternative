#include "test_framework.h"

int main() {
    std::cout << "ClickHouse ODBC Driver - Unit Tests" << std::endl;
    std::cout << "========================================" << std::endl;
    return test_framework::TestRunner::Instance().Run();
}
