# Sanitizer + warning helpers, applied to all RaftKV targets.

function(raftkv_apply_common_flags target)
  target_compile_features(${target} PUBLIC cxx_std_20)
  target_compile_options(${target} PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
    -Wno-unused-parameter)
  if(RAFTKV_SANITIZE STREQUAL "address")
    target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer -g)
    target_link_options(${target} PRIVATE -fsanitize=address,undefined)
  elseif(RAFTKV_SANITIZE STREQUAL "undefined")
    target_compile_options(${target} PRIVATE -fsanitize=undefined -fno-omit-frame-pointer -g)
    target_link_options(${target} PRIVATE -fsanitize=undefined)
  elseif(RAFTKV_SANITIZE STREQUAL "thread")
    target_compile_options(${target} PRIVATE -fsanitize=thread -fno-omit-frame-pointer -g)
    target_link_options(${target} PRIVATE -fsanitize=thread)
  endif()
endfunction()
