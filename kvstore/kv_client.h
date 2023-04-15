

#ifndef GRPC_KVSTORE_KV_CLIENT_H
#define GRPC_KVSTORE_KV_CLIENT_H

#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"
#include <string.h>


namespace kvstore {
    class KVClient {
    public:
        explicit KVClient(const std::string &addr) : stub_(
                KVStore::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()))) {
            LOG(INFO) << "Client is trying to connect to " << addr;
        }

        Status Scan(const std::string &start, std::vector<KV> &kvs,
                    size_t batch_size = std::numeric_limits<size_t>::max()) {
            ScanReq req;
            grpc::ClientContext cli_ctx;

            cli_ctx.set_wait_for_ready(true);
            if (!start.empty()) {
                req.set_start(start);
            }
            req.set_limit(batch_size);
            auto reader = stub_->Scan(&cli_ctx, req);
            ScanResp resp;
            Status status;

            while (reader->Read(&resp)) {
                kvs.push_back(resp.kv());
                
                // LOG(INFO) << "Scan key = " << resp.kv().key() << ", value length = " << resp.kv().value().size(); 
            }

            status.set_error_code(ErrorCode::OK);
            return status;
        }

        Status Get(const std::string &key, std::string &value) {
            GetReq req;
            GetResp resp;
            grpc::ClientContext cli_ctx;
            

            CHECK(key.size() <= 4 * 1024 * 1024);
            cli_ctx.set_wait_for_ready(true);
            req.set_key(key);
            auto grpc_status = stub_->Get(&cli_ctx, req, &resp);

            if (grpc_status.ok()) {
                if (resp.status().error_code() == ErrorCode::OK) {
                    value = resp.value();
                }
            } else {
                resp.mutable_status()->set_error_code(ErrorCode::CLIENT_ERROR);
                resp.mutable_status()->set_error_msg(grpc_status.error_message());
            }
            // LOG(INFO) << "Get key = " << key << ", value length = " << value.size(); 

            return resp.status();
        }

        void Warmup(const WarmupReq &req) {
            WarmupResp resp;
            grpc::ClientContext cli_ctx;

            cli_ctx.set_wait_for_ready(true);
            auto grpc_status = stub_->Warmup(&cli_ctx, req, &resp);
            CHECK(grpc_status.ok());
            // LOG(INFO) << "Warmup";
        }

        Status Put(const std::string &key, const std::string &value) {
            // LOG(INFO) << "Put";
            PutReq req;
            PutResp resp;
            grpc::ClientContext cli_ctx;

            CHECK(key.size() <= 4 * 1024 * 1024);
            CHECK(value.size() <= 4 * 1024 * 1024);
            *req.mutable_kv()->mutable_key() = key;
            *req.mutable_kv()->mutable_value() = value;

            cli_ctx.set_wait_for_ready(true);
            auto grpc_status = stub_->Put(&cli_ctx, req, &resp);
            if (!grpc_status.ok()) {
                resp.mutable_status()->set_error_code(ErrorCode::CLIENT_ERROR);
                resp.mutable_status()->set_error_msg(grpc_status.error_message());
            }

            // LOG(INFO) << "Put key = " << key << ", value length = " << value.size(); 
            return resp.status();
        }

        Status Delete(const std::string &key) {
            DeleteReq req;
            DeleteResp resp;
            grpc::ClientContext cli_ctx;

            CHECK(key.size() <= 4 * 1024 * 1024);
            req.set_key(key);
            auto grpc_status = stub_->Delete(&cli_ctx, req, &resp);

            if (!grpc_status.ok()) {
                resp.mutable_status()->set_error_code(ErrorCode::CLIENT_ERROR);
                resp.mutable_status()->set_error_msg(grpc_status.error_message());
            }

            // LOG(INFO) << "Delete key = " << key; 
            return resp.status();
        }

    private:
        std::unique_ptr<KVStore::Stub> stub_;
    };


}

#endif //GRPC_KVSTORE_KV_CLIENT_H
