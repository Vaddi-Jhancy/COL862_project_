cmake_minimum_required(VERSION 3.10)
project(sequencer)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include directories
set(INCLUDE_DIRS
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/generated
)

# Protobuf sources
set(PROTO_SRCS
    src/generated/sequencer.pb.cc
    src/generated/sequencer.grpc.pb.cc
    src/generated/sequencer_internal.pb.cc
    src/generated/sequencer_internal.grpc.pb.cc
)

############################################################
# Server (Leader + Followers)
############################################################
set(SERVER_SRCS
    src/sequencer.cpp
    src/sequencer_log.cpp
    src/sequencer_server.cpp
    src/main.cpp
    ${PROTO_SRCS}
)

add_executable(sequencer ${SERVER_SRCS})
target_include_directories(sequencer PRIVATE ${INCLUDE_DIRS})

# ------------------------------
# Protobuf
# ------------------------------
find_package(Protobuf REQUIRED)

# ------------------------------
# ZooKeeper (Ubuntu: libzookeeper-mt-dev)
# Ubuntu installs headers in: /usr/include/zookeeper
# and library: libzookeeper_mt.so in standard lib path
# ------------------------------
include_directories(/usr/include/zookeeper)

target_link_libraries(sequencer
    PRIVATE
        grpc++
        grpc
        gpr
        ${Protobuf_LIBRARIES}
        pthread
        zookeeper_mt         # <-- Manually link ZooKeeper here
)

############################################################
# Client (No ZooKeeper Needed)
############################################################
set(CLIENT_SRCS
    client/append_client.cpp
    src/generated/sequencer.pb.cc
    src/generated/sequencer.grpc.pb.cc
)

add_executable(append_client ${CLIENT_SRCS})
target_include_directories(append_client PRIVATE ${INCLUDE_DIRS})

target_link_libraries(append_client
    PRIVATE
        grpc++
        grpc
        gpr
        ${Protobuf_LIBRARIES}
        pthread
)
