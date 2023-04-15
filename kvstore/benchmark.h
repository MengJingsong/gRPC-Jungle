#ifndef GRPC_KVSTORE_BENCHMARK_H
#define GRPC_KVSTORE_BENCHMARK_H

#include "glog/logging.h"
#include "kv_client.h"
#include "stopwatch.h"
#include "common.h"
#include <string.h>


namespace kvstore {
    std::string gen_random_string(size_t len) {
        static const char alphanum[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
        std::string tmp_s;
        tmp_s.reserve(len);

        for (int i = 0; i < len; ++i) {
            tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
        }

        return tmp_s;
    }

    void TestScan(const std::shared_ptr<KVClient> &kv_cli,
                  size_t batch_size) {
        Stopwatch sw;
        std::vector<KV> kvs;
        kvs.reserve(1000000);
        size_t size_in_byte = 0;

        sw.start();
        Status status = kv_cli->Scan("", kvs, batch_size);
        sw.stop();

        if (status.error_code() != ErrorCode::OK) {
            LOG(FATAL) << "GetBatch Error: " << status.error_code() << " msg: " << status.error_msg();
        }

        for (auto &kv: kvs) {
            size_in_byte += kv.key().size() + kv.value().size();
        }

        LOG(INFO) << kvs.size() << " kvs are found, total data size: " << (float) size_in_byte / 1024.0 / 1024.0
                  << " MB";
        LOG(INFO) << "Time: " << sw.ms() << " ms, avg: " << kvs.size() / (sw.ms() / 1000) << " kv/s";
    }

    void TestGet(const std::shared_ptr<KVClient> &kv_cli,
                 size_t batch_size) {
        Stopwatch sw;
        std::vector<KV> kvs;
        Status status;
        size_t size_in_byte = 0;

        status = kv_cli->Scan("", kvs, batch_size);
        if (status.error_code() != ErrorCode::OK) {
            LOG(FATAL) << "GetBatch Error: " << status.error_code() << " msg: " << status.error_msg();
        }
        sw.start();
        for (auto &kv: kvs) {
            std::string value;

            status = kv_cli->Get(kv.key(), value);
            if (status.error_code() != ErrorCode::OK) {
                LOG(FATAL) << "Failed to get key " << kv.key() << " ErrorCode: " << status.error_code() << " msg: " <<
                           status.error_msg();
            }
            CHECK_EQ(kv.value(), value) << "Value does not match with the value from GetBatch: " << value << " vs "
                                        << kv.value();
            size_in_byte += kv.key().size() + kv.value().size();
        }
        sw.stop();
        LOG(INFO) << kvs.size() << " kvs are found, total data size: " << (float) size_in_byte / 1024.0 / 1024.0
                  << " MB";
        LOG(INFO) << "Time: " << sw.ms() << " ms, avg: " << kvs.size() / (sw.ms() / 1000) << " kv/s";
    }

    void TestPut(const std::shared_ptr<KVClient> &kv_cli,
                 size_t key_size, size_t max_val_size, size_t batch_size,
                 bool variable_value_size) {
        Stopwatch sw;
        std::vector<std::pair<std::string, std::string>> reqs;
        size_t size_in_byte = 0;
        reqs.reserve(batch_size);

        for (size_t i = 0; i < batch_size; i++) {
            std::string key = gen_random_string(key_size), value;
            auto val_size = variable_value_size ? random(1, max_val_size) : max_val_size;

            value.resize(val_size);
            reqs.emplace_back(std::make_pair(key, value));
            size_in_byte += key_size + val_size;
        }

        sw.start();
        for (size_t i = 0; i < batch_size; i++) {
            auto &req = reqs[i];
            auto status = kv_cli->Put(req.first, req.second);

            if (status.error_code() != ErrorCode::OK) {
                LOG(FATAL) << "Put Error: " << status.error_code() << " msg: " << status.error_msg();
            }
        }
        sw.stop();

        LOG(INFO) << batch_size << " kvs are inserted, max Value size: " << max_val_size << " Batch size: "
                  << batch_size
                  << " Total data size: " << (float) size_in_byte / 1024.0 / 1024.0 << " MB";
        LOG(INFO) << "Time: " << sw.ms() << " ms, avg: " << batch_size / (sw.ms() / 1000) << " kv/s";
    }

    void TestDelete(const std::shared_ptr<KVClient> &kv_cli,
                    size_t batch_size) {
        Stopwatch sw;
        std::vector<KV> kvs;
        Status status = kv_cli->Scan("", kvs, batch_size);
        size_t size_in_byte = 0;

        if (status.error_code() != ErrorCode::OK) {
            LOG(FATAL) << "GetBatch Error: " << status.error_code() << " msg: " << status.error_msg();
        }

        sw.start();
        for (auto &kv: kvs) {
            status = kv_cli->Delete(kv.key());
            if (status.error_code() != ErrorCode::OK) {
                LOG(FATAL) << "Failed to get key " << kv.key() << " ErrorCode: " << status.error_code() << " msg: " <<
                           status.error_msg();
            }
            size_in_byte += kv.key().size() + kv.value().size();
        }
        sw.stop();
        LOG(INFO) << kvs.size() << " kvs are deleted, total data size: " << (float) size_in_byte / 1024.0 / 1024.0
                  << " MB";
        LOG(INFO) << "Time: " << sw.ms() << " ms, avg: " << kvs.size() / (sw.ms() / 1000) << " kv/s";
    }

    void Warmup(const std::shared_ptr<KVClient> &kv_cli,
                size_t size_in_byte,
                bool big_req,
                bool big_resp) {
        Stopwatch sw;
        WarmupReq req;
        std::string val;

        size_in_byte = random(1, size_in_byte);
        size_t total_bytes = 0;

        if (big_req) {
            req.mutable_data()->resize(size_in_byte);
            total_bytes += size_in_byte;
        } else {
            req.mutable_data()->resize(1);
            total_bytes += 1;
        }
        if (big_resp) {
            req.set_resp_size(size_in_byte);
            total_bytes += size_in_byte;
        } else {
            req.set_resp_size(0);
        }

        sw.start();
        kv_cli->Warmup(req);
        sw.stop();
        LOG(INFO) << "Time: " << sw.ms() << " ms, Size: " << total_bytes << " Bandwidth: "
                  << (float) total_bytes / 1024 / 1024 / (sw.ms() / 1000) << " MB/s";
    }
}

#endif //GRPC_KVSTORE_BENCHMARK_H
