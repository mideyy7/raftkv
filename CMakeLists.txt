cmake_minimum_required(VERSION 3.20)
project(raftkv CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

set(RAFTKV_SANITIZE "none" CACHE STRING "none|address|undefined|thread")
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include(cmake/Sanitizers.cmake)
enable_testing()

# ---------------------------------------------------------------------------
# core library
# ---------------------------------------------------------------------------
set(RAFTKV_SOURCES
  src/common/crc32c.cpp
  src/common/config.cpp
  src/storage/wal.cpp
  src/storage/log_store.cpp
  src/storage/kv_table.cpp
  src/storage/store_engine.cpp
  # phase 2+
  src/raft/raft_core.cpp
  src/transport/inprocess.cpp
  src/transport/test_network.cpp
  src/transport/tcp.cpp
  src/node/raft_node.cpp
  # phase 4+
  src/storage/session_cache.cpp
  src/client/raftkv_client.cpp
)

# Keep only sources that already exist so the tree builds phase-by-phase.
set(_present "")
foreach(_s ${RAFTKV_SOURCES})
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${_s}")
    list(APPEND _present ${_s})
  endif()
endforeach()

add_library(raftkv STATIC ${_present})
target_include_directories(raftkv PUBLIC src)
target_link_libraries(raftkv PUBLIC Threads::Threads)
raftkv_apply_common_flags(raftkv)

# ---------------------------------------------------------------------------
# helper for test / tool executables
# ---------------------------------------------------------------------------
function(raftkv_add_test name src label)
  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${src}")
    return()
  endif()
  add_executable(${name} ${src})
  target_include_directories(${name} PRIVATE src tools tests)
  target_link_libraries(${name} PRIVATE raftkv)
  raftkv_apply_common_flags(${name})
  add_test(NAME ${name} COMMAND ${name})
  set_tests_properties(${name} PROPERTIES LABELS "${label}" TIMEOUT 300)
endfunction()

function(raftkv_add_tool name src)
  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${src}")
    return()
  endif()
  add_executable(${name} ${src})
  target_include_directories(${name} PRIVATE src tools)
  target_link_libraries(${name} PRIVATE raftkv)
  raftkv_apply_common_flags(${name})
endfunction()

# ---- phase 1 ----
raftkv_add_test(t_crc32c            tests/unit/crc32c_test.cpp          phase1)
raftkv_add_test(t_codec             tests/unit/codec_test.cpp           phase1)
raftkv_add_test(t_wal               tests/unit/wal_test.cpp             phase1)
raftkv_add_test(t_log_store         tests/unit/log_store_test.cpp       phase1)
raftkv_add_test(t_kv_table          tests/unit/kv_table_test.cpp        phase1)
raftkv_add_test(t_store_engine      tests/unit/store_engine_test.cpp    phase1)
raftkv_add_tool(raftkv-store        src/server/store_main.cpp)

# ---- phase 2 ----
raftkv_add_test(t_raft_election     tests/unit/raft_election_test.cpp   phase2)
raftkv_add_test(t_election_integ    tests/integration/election_test.cpp phase2)
raftkv_add_tool(raftkv-node         src/server/node_main.cpp)

# ---- phase 3 ----
raftkv_add_test(t_raft_replication  tests/unit/raft_replication_test.cpp phase3)
raftkv_add_test(t_replication_integ tests/integration/replication_test.cpp phase3)
raftkv_add_tool(raftkv-fuzz         tests/fuzz/raft_fuzz.cpp)

# ---- phase 4 ----
raftkv_add_test(t_session_cache     tests/unit/session_cache_test.cpp   phase4)
raftkv_add_test(t_client            tests/unit/client_test.cpp          phase4)
raftkv_add_test(t_client_integ      tests/integration/client_test.cpp   phase4)
raftkv_add_tool(raftkv-cli          src/client/cli_main.cpp)

# ---- phase 5 ----
raftkv_add_tool(raftkv-loadgen      bench/loadgen.cpp)
