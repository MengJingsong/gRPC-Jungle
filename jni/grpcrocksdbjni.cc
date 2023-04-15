#include "site_ycsb_db_grpc_rocksdb_GRPCRocksDBClient.h"
#include <string>
#include "kv_client.h"
#include "common.h"

std::string jstring2string(JNIEnv *env, jstring jStr) {
    if (!jStr)
        return "";

    const jclass stringClass = env->GetObjectClass(jStr);
    const jmethodID getBytes = env->GetMethodID(stringClass, "getBytes", "(Ljava/lang/String;)[B");
    const jbyteArray stringJbytes = (jbyteArray) env->CallObjectMethod(jStr, getBytes, env->NewStringUTF("UTF-8"));

    size_t length = (size_t) env->GetArrayLength(stringJbytes);
    jbyte *pBytes = env->GetByteArrayElements(stringJbytes, NULL);

    std::string ret = std::string((char *) pBytes, length);
    env->ReleaseByteArrayElements(stringJbytes, pBytes, JNI_ABORT);

    env->DeleteLocalRef(stringJbytes);
    env->DeleteLocalRef(stringClass);
    return ret;
}

jbyteArray to_jbyte_array(JNIEnv *env, const std::string &s) {
    jsize len = s.length();
    if (len >= 4 * 1024 * 1024) {
        printf("Too large %d\n", len);
        exit(1);
    }
    jbyteArray arr = env->NewByteArray(len);
    env->SetByteArrayRegion(arr, 0, len, reinterpret_cast<const jbyte *>(s.c_str()));
    return arr;
}

std::string to_string(JNIEnv *env, jbyteArray data) {
    jboolean is_copy;
    jbyte *b = env->GetByteArrayElements(data, &is_copy);
    jsize len = env->GetArrayLength(data);

    std::string ret = std::string(reinterpret_cast<char *>(b), len);
    env->ReleaseByteArrayElements(data, b, JNI_ABORT);
    return ret;
}

jobject new_array_list(JNIEnv *env, jsize size) {
    auto array_list_clz = static_cast<jclass>(env->NewGlobalRef(env->FindClass("java/util/ArrayList")));
    auto array_list_constructor = env->GetMethodID(array_list_clz, "<init>", "(I)V");
    return env->NewObject(array_list_clz, array_list_constructor, size);
}

JNIEXPORT jlong JNICALL Java_site_ycsb_db_grpc_rocksdb_GRPCRocksDBClient_connect
        (JNIEnv *j_env, jobject j_obj, jstring j_addr) {
    auto addr = jstring2string(j_env, j_addr);
    std::string val;
    auto kv_cli = new kvstore::KVClient(addr);

    for (int i = 0; i < 10000; i++) {
        kvstore::WarmupReq req;

        req.mutable_data()->resize(kvstore::random(1, 1024));
        req.set_resp_size(kvstore::random(1, 1024));

        kv_cli->Warmup(req);
    }

    return reinterpret_cast<jlong>(kv_cli);
}

JNIEXPORT jbyteArray JNICALL Java_site_ycsb_db_grpc_rocksdb_GRPCRocksDBClient_get
        (JNIEnv *j_env, jobject j_obj, jlong j_handle, jbyteArray j_key) {
    auto *cli = reinterpret_cast<kvstore::KVClient *>(j_handle);
    std::string val;
    auto status = cli->Get(to_string(j_env, j_key), val);

    if (status.error_code() == kvstore::ErrorCode::OK) {
        return to_jbyte_array(j_env, val);
    } else {
        std::cout << status.error_msg() << std::endl;
    }
    return nullptr;
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_grpc_rocksdb_GRPCRocksDBClient_put
        (JNIEnv *j_env, jobject j_obj, jlong j_handle, jbyteArray j_key, jbyteArray j_value) {
    auto *cli = reinterpret_cast<kvstore::KVClient *>(j_handle);
    auto status = cli->Put(to_string(j_env, j_key), to_string(j_env, j_value));
    return status.error_code() == kvstore::ErrorCode::OK ? 0 : status.error_code();
}

JNIEXPORT jint JNICALL Java_site_ycsb_db_grpc_rocksdb_GRPCRocksDBClient_delete
        (JNIEnv *j_env, jobject j_obj, jlong j_handle, jbyteArray j_key) {
    auto *cli = reinterpret_cast<kvstore::KVClient *>(j_handle);
    auto err = cli->Delete(to_string(j_env, j_key)).error_code();
    return err == kvstore::ErrorCode::OK ? 0 : err;
}

JNIEXPORT jobject JNICALL Java_site_ycsb_db_grpc_rocksdb_GRPCRocksDBClient_scan
        (JNIEnv *j_env, jobject j_obj, jlong j_handle, jbyteArray j_key, jint j_limit, jboolean j_return_keys) {
    auto *cli = reinterpret_cast<kvstore::KVClient *>(j_handle);
    std::vector<kvstore::KV> kvs;

    cli->Scan(to_string(j_env, j_key), kvs, j_limit);

    jclass clazz = j_env->FindClass("site/ycsb/db/grpc/rocksdb/GRPCRocksDBClient$KVPairs");
    assert(clazz != nullptr);
    jmethodID constructor = j_env->GetMethodID(clazz, "<init>", "()V");
    assert(constructor != nullptr);
    jfieldID j_keys_id = j_env->GetFieldID(clazz, "keys", "Ljava/util/List;");
    jfieldID j_vals_id = j_env->GetFieldID(clazz, "values", "Ljava/util/List;");
    assert(j_keys_id != nullptr && j_vals_id != nullptr);

    auto j_keys = new_array_list(j_env, kvs.size());
    auto j_vals = new_array_list(j_env, kvs.size());
    auto array_list_clz = static_cast<jclass>(j_env->NewGlobalRef(j_env->FindClass("java/util/ArrayList")));
    auto list_add = j_env->GetMethodID(array_list_clz, "add", "(Ljava/lang/Object;)Z");

    for (auto &kv: kvs) {
        if (j_return_keys) {
            auto j_bytes = to_jbyte_array(j_env, kv.key());
            j_env->CallBooleanMethod(j_keys, list_add, j_bytes);
            j_env->DeleteLocalRef(j_bytes);
        }
        auto j_bytes = to_jbyte_array(j_env, kv.value());
        j_env->CallBooleanMethod(j_vals, list_add, j_bytes);
        j_env->DeleteLocalRef(j_bytes);
    }


    jobject obj = j_env->NewObject(clazz, constructor);

    if (j_return_keys) {
        j_env->SetObjectField(obj, j_keys_id, j_keys);
    }
    j_env->SetObjectField(obj, j_vals_id, j_vals);
    return obj;
}

JNIEXPORT void JNICALL Java_site_ycsb_db_grpc_rocksdb_GRPCRocksDBClient_disconnect
        (JNIEnv *j_env, jobject j_obj, jlong j_handle) {
    auto *cli = reinterpret_cast<kvstore::KVClient *>(j_handle);
    delete cli;
}