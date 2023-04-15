cmake_minimum_required(VERSION 3.21)
project(gRPC_KVStore)

set(CMAKE_CXX_STANDARD 17)

include(cmake/FindGFlags.cmake)
if (${GFLAGS_FOUND})
    include_directories(${GFLAGS_INCLUDE_DIRS})
else ()
    message(FATAL_ERROR "no gflags")
endif ()

include(cmake/FindGlog.cmake)
if (${GLOG_FOUND})
    include_directories(${GLOG_INCLUDE_DIRS})
else ()
    message(FATAL_ERROR "no glogs")
endif ()

# jungle
add_subdirectory("${PROJECT_SOURCE_DIR}/Jungle")
include_directories(${PROJECT_SOURCE_DIR}/Jungle/include)

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
if (CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
else ()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif ()

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if (CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else ()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif ()

find_package(JNI)

if (JNI_FOUND)
    message(STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
    message(STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
    include_directories(${JNI_INCLUDE_DIRS})
endif ()

# Proto file
get_filename_component(kv_proto "protos/kvstore.proto" ABSOLUTE)
get_filename_component(kv_proto_path "${kv_proto}" PATH)

# Generated sources
set(kv_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/kvstore.pb.cc")
set(kv_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/kvstore.pb.h")
set(kv_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/kvstore.grpc.pb.cc")
set(kv_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/kvstore.grpc.pb.h")
add_custom_command(
        OUTPUT "${kv_proto_srcs}" "${kv_proto_hdrs}" "${kv_grpc_srcs}" "${kv_grpc_hdrs}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${kv_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${kv_proto}"
        DEPENDS "${kv_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# kv_grpc_proto
add_library(kv_grpc_proto STATIC
        ${kv_grpc_srcs}
        ${kv_grpc_hdrs}
        ${kv_proto_srcs}
        ${kv_proto_hdrs})
set_property(TARGET kv_grpc_proto PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_libraries(kv_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})


add_executable(kv_store kvstore/kv_store.cc kvstore/flags.cc)
target_include_directories(kv_store PRIVATE kvstore)
target_link_libraries(kv_store ${GFLAGS_LIBRARIES} ${GLOG_LIBRARIES} static_lib
        kv_grpc_proto
        ${_REFLECTION}
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF})
if (NOT APPLE)
    target_link_libraries(kv_store ibverbs)
endif ()

if(JNI_FOUND)
    add_library(grpcrocksdbjni SHARED jni/grpcrocksdbjni.cc)
    target_include_directories(grpcrocksdbjni PRIVATE kvstore jni ${MPI_CXX_INCLUDE_PATH})
    target_link_libraries(grpcrocksdbjni ${GLOG_LIBRARIES}
            kv_grpc_proto
            ${_REFLECTION}
            ${_GRPC_GRPCPP}
            ${_PROTOBUF_LIBPROTOBUF})
    if (NOT APPLE)
        target_link_libraries(grpcrocksdbjni ibverbs)
    endif ()
endif()