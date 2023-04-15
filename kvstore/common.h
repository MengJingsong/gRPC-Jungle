

#ifndef GRPC_KVSTORE_COMMON_H
#define GRPC_KVSTORE_COMMON_H

#include <math.h>
#include <ctime>

namespace kvstore {
    int random(size_t min, size_t max) { //range : [min, max]
        static bool first = true;
        if (first) {
            srand(time(NULL)); //seeding for the first time only!
            first = false;
        }
        return min + rand() % ((max + 1) - min);
    }
}
#endif //GRPC_KVSTORE_COMMON_H
