#include "flags.h"
#include "gflags/gflags.h"

DEFINE_string(db_file, "./jungle_db", "");
DEFINE_string(addr, "0.0.0.0", "");
DEFINE_int32(port, 12345, "");
DEFINE_bool(server, false, "is server or client");
DEFINE_bool(async, false, "Async server");
DEFINE_int32(thread, 28, "Thread number for serving");
DEFINE_int32(repeat, 10, "repeat times");
DEFINE_uint32(key_size, 64, "key size in bytes");
DEFINE_uint32(val_size, 1024, "value size");
DEFINE_uint32(batch_size, 100000, "Batch size");
DEFINE_bool(variable, true, "Random value size");
DEFINE_string(cmd, "put", "get/get_batch/put");
DEFINE_int32(big_kv_in_kb, 4, "kv size in kb");
DEFINE_bool(big_k, true, "");
DEFINE_bool(big_v, false, "");
DEFINE_bool(warmup, true, "");