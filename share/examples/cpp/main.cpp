// CoreTexDB C++ example (links against libcoretexdb + include/coretexdb.h)
#include "coretexdb.h"
#include <iostream>

int main() {
    std::cout << "CoreTexDB " << CORETEXDB_VERSION_MAJOR << "."
              << CORETEXDB_VERSION_MINOR << "."
              << CORETEXDB_VERSION_PATCH << std::endl;
    return 0;
}
