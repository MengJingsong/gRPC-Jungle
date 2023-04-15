#ifndef GRPC_KVSTORE_KV_SERVER_H
#define GRPC_KVSTORE_KV_SERVER_H

#include <typeinfo>
#include <libjungle/jungle.h>
#include <utility>
#include <grpcpp/server_builder.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <string.h>

#include "glog/logging.h"
#include "flags.h"
#include "kvstore.grpc.pb.h"
#include "common.h"

namespace kvstore {
    class KVStoreServiceImpl final : public KVStore::Service {
        public:

        explicit KVStoreServiceImpl(jungle::DB *db) : db_(db) {}

        ::grpc::Status
        Get(::grpc::ServerContext *context, const ::kvstore::GetReq *request, ::kvstore::GetResp *response) override {
            // LOG(INFO) << "Get";
            auto &key = request->key();

            jungle::SizedBuf value_out;

            jungle::Status s = db_->get(jungle::SizedBuf(key), value_out);

            *(response->mutable_value()) = value_out.toString();

            return wrapStatus(s, response->mutable_status());
        }

        ::grpc::Status
        Put(::grpc::ServerContext *context, const ::kvstore::PutReq *request, ::kvstore::PutResp *response) override {
            // LOG(INFO) << "Put";
            auto &kv = request->kv();
            jungle::Status s = db_->set(jungle::KV(kv.key(), kv.value()));
            return wrapStatus(s, response->mutable_status());
        }

        ::grpc::Status Delete(::grpc::ServerContext *context, const ::kvstore::DeleteReq *request,
                              ::kvstore::DeleteResp *response) override {
            // LOG(INFO) << "Delete";
            auto &key = request->key();
            jungle::Status s = db_->del(jungle::SizedBuf(key));
            return wrapStatus(s, response->mutable_status());
        }

        ::grpc::Status Scan(::grpc::ServerContext *context, const ::kvstore::ScanReq *request,
                            ::grpc::ServerWriter<::kvstore::ScanResp> *writer) override {
            // LOG(INFO) << "Scan";
            size_t batch_size = request->has_limit() ? request->limit() : std::numeric_limits<size_t>::max();
            // auto *it = db_->NewIterator(rocksdb::ReadOptions());
            jungle::Iterator iter;
            iter.init(db_);
            do {
                jungle::Record rec_out;
                jungle::Status s = iter.get(rec_out);
                if (!s) break;

                ScanResp resp;
                resp.mutable_kv()->mutable_key()->assign(rec_out.kv.key.toString().data(), rec_out.kv.key.toString().size());
                resp.mutable_kv()->mutable_value()->assign(rec_out.kv.value.toString().data(), rec_out.kv.value.toString().size());

                writer->Write(resp);
            } while(iter.next().ok() && batch_size > 0);
            iter.close();
            return grpc::Status::OK;
        }

        ::grpc::Status
        Warmup(::grpc::ServerContext *context, const ::kvstore::WarmupReq *request,
               ::kvstore::WarmupResp *response) override {
            // LOG(INFO) << "Warmup";
            response->mutable_data()->resize(request->resp_size());
            return grpc::Status::OK;
        }

        private:

        jungle::DB *db_;

        static grpc::Status wrapStatus(const jungle::Status &jdb_status, Status *status) {
            if (jdb_status.ok()) {
                status->set_error_code(ErrorCode::OK);
            } else {
                status->set_error_code(ErrorCode::SERVER_ERROR);
                // status->set_error_msg(jdb_status.toString());
            }
            return grpc::Status::OK;
        }
    };

    enum class CallStatus {
        CREATE, PROCESS, WRITING, FINISH
    };

    class Call {
    public:
        Call(KVStore::AsyncService *service,
             grpc::ServerCompletionQueue *cq,
             jungle::DB *db) : service_(service), cq_(cq), db_(db), call_status_(CallStatus::CREATE) {
            // LOG(INFO) << "Call::Call";
        }

        virtual ~Call() = default;

        virtual void Proceed() = 0;

    protected:
        KVStore::AsyncService *service_;
        grpc::ServerCompletionQueue *cq_;
        grpc::ServerContext ctx_;

        jungle::DB *db_;
        CallStatus call_status_;

        static grpc::Status wrapStatus(const jungle::Status &jdb_status, Status *status) {
            if (jdb_status.ok()) {
                status->set_error_code(ErrorCode::OK);
            } else {
                status->set_error_code(ErrorCode::SERVER_ERROR);
            }
            return grpc::Status::OK;
        }
    };

    class GetCall : public Call {
    public:
        GetCall(KVStore::AsyncService *service,
                grpc::ServerCompletionQueue *cq,
                jungle::DB *db) :
                Call(service, cq, db), responder_(&ctx_) {
            // LOG(INFO) << "GetCall::GetCall";
            call_status_ = CallStatus::PROCESS;
            service_->RequestGet(&ctx_, &req_, &responder_, cq_, cq_, this);
        }

        void Proceed() override {
            // LOG(INFO) << "GetCall::Proceed";
            if (call_status_ == CallStatus::PROCESS) {
                new GetCall(service_, cq_, db_);

                auto &key = req_.key();

                jungle::SizedBuf value_out;

                jungle::Status s = db_->get(jungle::SizedBuf(key), value_out);

                *(resp_.mutable_value()) = value_out.toString();

                call_status_ = CallStatus::FINISH;
                responder_.Finish(resp_, wrapStatus(s, resp_.mutable_status()), this);
            } else {
                GPR_ASSERT(call_status_ == CallStatus::FINISH);
                delete this;
            }
        }

    private:
        GetReq req_;
        GetResp resp_;
        grpc::ServerAsyncResponseWriter<GetResp> responder_;
    };

    class WarmupCall : public Call {
    public:
        WarmupCall(KVStore::AsyncService *service,
                   grpc::ServerCompletionQueue *cq,
                   jungle::DB *db) :
                Call(service, cq, db), responder_(&ctx_) {
            // LOG(INFO) << "WarmupCall::WarmupCall";
            call_status_ = CallStatus::PROCESS;
            service_->RequestWarmup(&ctx_, &req_, &responder_, cq_, cq_, this);
        }

        void Proceed() override {
            // LOG(INFO) << "WarmupCall::Proceed";
            if (call_status_ == CallStatus::PROCESS) {
                new WarmupCall(service_, cq_, db_);
                resp_.mutable_data()->resize(req_.resp_size());
                call_status_ = CallStatus::FINISH;
                responder_.Finish(resp_, grpc::Status::OK, this);
            } else {
                GPR_ASSERT(call_status_ == CallStatus::FINISH);
                delete this;
            }
        }

    private:
        WarmupReq req_;
        WarmupResp resp_;
        grpc::ServerAsyncResponseWriter<WarmupResp> responder_;
    };

    class PutCall : public Call {
    public:
        PutCall(KVStore::AsyncService *service,
                grpc::ServerCompletionQueue *cq,
                jungle::DB *db) :
                Call(service, cq, db), responder_(&ctx_) {
            // LOG(INFO) << "PutCall::PutCall";
            call_status_ = CallStatus::PROCESS;
            service_->RequestPut(&ctx_, &req_, &responder_, cq_, cq_, this);
        }

        void Proceed() override {
            // LOG(INFO) << "PutCall::Proceed";
            if (call_status_ == CallStatus::PROCESS) {
                new PutCall(service_, cq_, db_);
                auto &kv = req_.kv();
                jungle::Status s = db_->set(jungle::KV(kv.key(), kv.value()));
                call_status_ = CallStatus::FINISH;
                responder_.Finish(resp_, wrapStatus(s, resp_.mutable_status()), this);
            } else {
                GPR_ASSERT(call_status_ == CallStatus::FINISH);
                delete this;
            }
        }

    private:
        PutReq req_;
        PutResp resp_;
        grpc::ServerAsyncResponseWriter<PutResp> responder_;
    };

    class DeleteCall : public Call {
    public:
        DeleteCall(KVStore::AsyncService *service,
                   grpc::ServerCompletionQueue *cq,
                   jungle::DB *db) :
                Call(service, cq, db), responder_(&ctx_) {
            // LOG(INFO) << "DeleteCall::DeleteCall";
            call_status_ = CallStatus::PROCESS;
            service_->RequestDelete(&ctx_, &req_, &responder_, cq_, cq_, this);
        }

        void Proceed() override {
            // LOG(INFO) << "DeleteCall::Proceed";
            if (call_status_ == CallStatus::PROCESS) {
                new DeleteCall(service_, cq_, db_);

                auto &key = req_.key();
                jungle::Status s = db_->del(jungle::SizedBuf(key));

                call_status_ = CallStatus::FINISH;
                responder_.Finish(resp_, wrapStatus(s, resp_.mutable_status()), this);
            } else {
                GPR_ASSERT(call_status_ == CallStatus::FINISH);
                delete this;
            }
        }

    private:
        DeleteReq req_;
        DeleteResp resp_;
        grpc::ServerAsyncResponseWriter<DeleteResp> responder_;
    };

    class ScanCall : public Call {
    public:
        ScanCall(KVStore::AsyncService *service,
                 grpc::ServerCompletionQueue *cq,
                 jungle::DB *db) :
                Call(service, cq, db), writer_(&ctx_) {
            // LOG(INFO) << "ScanCall::ScanCall";
            call_status_ = CallStatus::PROCESS;
            service_->RequestScan(&ctx_, &req_, &writer_, cq_, cq_, this);
        }

        void Proceed() override {
            // LOG(INFO) << "ScanCall::Proceed";
            if (call_status_ == CallStatus::PROCESS) {
                new ScanCall(service_, cq_, db_);
                rest_size_ = req_.has_limit() ? req_.limit() : std::numeric_limits<size_t>::max();
                it_ = new jungle::Iterator();
                if (req_.has_start()) {
                    it_->init(db_, jungle::SizedBuf(req_.start()));
                } else {
                    it_->init(db_);
                }
                call_status_ = CallStatus::WRITING;
                write();
            } else if (call_status_ == CallStatus::WRITING) {
                write();
            } else if (call_status_ == CallStatus::FINISH) {
                delete it_;

                delete this;
            }
        }

    private:
        ScanReq req_;
        grpc::ServerAsyncWriter<ScanResp> writer_;
        size_t rest_size_{};
        jungle::Iterator *it_{};

        void write() {

            jungle::Record rec_out;
            jungle::Status s = it_->get(rec_out);
            if (s.ok() && rest_size_ > 0) {
                ScanResp resp;
                resp.mutable_kv()->mutable_key()->assign(rec_out.kv.key.toString().data(), rec_out.kv.key.toString().size());
                resp.mutable_kv()->mutable_value()->assign(rec_out.kv.value.toString().data(), rec_out.kv.value.toString().size());
                writer_.Write(resp, this);
                it_->next();
                rest_size_--;
            } else {
                call_status_ = CallStatus::FINISH;
                writer_.Finish(grpc::Status::OK, this);
            }
        }
    };


    class KVServer {
    public:
        explicit KVServer(const std::string &db_path) {
            db_ = createAndOpenDB(db_path);
        }

        virtual ~KVServer() = default;

        virtual void Start() = 0;

        virtual void Stop() {
            if (db_ != nullptr) {
                closeDB(db_);
            }
        }

        jungle::DB *get_db() {
            return db_;
        }

    private:
        jungle::DB *db_{};

        static jungle::DB *createAndOpenDB(const std::string& path) {
            // LOG(INFO) << "createAndOpenDB " << path;

            jungle::init(jungle::GlobalConfig());
            jungle::DB *db = nullptr;
            jungle::DB::open(&db, path, jungle::DBConfig());

            // jungle::Iterator iter;
            // iter.init(db);
            // do {
            //     jungle::Record rec_out;
            //     jungle::Status s = iter.get(rec_out);
            //     if (!s) break;

            //     LOG(INFO) << "createAndOpenDB iterate: key = " << rec_out.kv.key.toString() << ", value length = " << rec_out.kv.value.toString().size();
            // } while(iter.next().ok());
            // iter.close();

            LOG(INFO) << "createAndOpenDB " << path;

            return db;
        }

        static void closeDB(jungle::DB *db) {
            jungle::DB::close(db);
            jungle::shutdown();
        }
    };


    class KVServerSync : public KVServer {
    public:
        KVServerSync(const std::string &db_file, std::string addr) :
                KVServer(db_file),
                addr_(std::move(addr)) {

        }

        void Start() override {
            sync_service_ = std::make_unique<KVStoreServiceImpl>(get_db());
            grpc::EnableDefaultHealthCheckService(true);
            grpc::reflection::InitProtoReflectionServerBuilderPlugin();
            grpc::ServerBuilder builder;
            builder.AddListeningPort(addr_, grpc::InsecureServerCredentials());
            builder.RegisterService(sync_service_.get());
            server_ = builder.BuildAndStart();
            LOG(INFO) << "Sync Server is listening on " << addr_;
            server_->Wait();
        }

        void Stop() override {
            server_->Shutdown();
            KVServer::Stop();
        }

    private:
        std::string addr_;
        std::unique_ptr<KVStoreServiceImpl> sync_service_;
        std::unique_ptr<grpc::Server> server_;
    };


    class KVServerAsync : public KVServer {
    public:
        KVServerAsync(const std::string &db_file, std::string addr,
                      int num_thread) :
                KVServer(db_file),
                addr_(std::move(addr)),
                num_thread_(num_thread) {

        }

        void Start() override {
            grpc::EnableDefaultHealthCheckService(true);
            grpc::reflection::InitProtoReflectionServerBuilderPlugin();
            grpc::ServerBuilder builder;
            builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_ALLOW_REUSEPORT, 0));
            builder.AddListeningPort(addr_, grpc::InsecureServerCredentials());
            builder.RegisterService(&service_);
            for (int i = 0; i < num_thread_; i++) {
                cqs_.emplace_back(builder.AddCompletionQueue());
            }
            LOG(INFO) << "Async Server is listening on " << addr_ << " Serving thread: " << num_thread_;
            server_ = builder.BuildAndStart();
            auto *db = get_db();
            std::vector<std::thread> ths;

            for (int i = 0; i < num_thread_; i++) {
                auto *cq = cqs_[i].get();
                new GetCall(&service_, cq, db);
                new PutCall(&service_, cq, db);
                new DeleteCall(&service_, cq, db);
                new ScanCall(&service_, cq, db);
                new WarmupCall(&service_, cq, db);

                ths.emplace_back([cq](int tid) {
                    void *tag;
                    bool ok;
                    while (cq->Next(&tag, &ok)) {
                        if (ok) {
                            static_cast<Call *>(tag)->Proceed();
                        }
                    }
                }, i);
            }
            for (auto &th: ths) {
                th.join();
            }
        }

        void Stop() override {
            server_->Shutdown();
            for (auto &cq: cqs_) {
                cq->Shutdown();
            }

            KVServer::Stop();
        }

    private:
        std::string addr_;
        KVStore::AsyncService service_;
        std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
        std::unique_ptr<grpc::Server> server_;
        int num_thread_;
    };


}
#endif //GRPC_KVSTORE_KV_SERVER_H