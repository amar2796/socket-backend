cmake_minimum_required(VERSION 3.15)
project(socktest-backend CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Use the system-installed Boost and nlohmann-json (installed via apt in the
# Dockerfile) instead of FetchContent-ing and compiling Boost from source on
# every single Docker build. The old approach downloaded ~100MB and rebuilt
# Boost from scratch every deploy, which is slow and can hit Render's free
# tier build time limits. Boost.Beast/Asio are header-only, so we only need
# libboost-system for the small compiled bits Beast/Asio still rely on.
find_package(Boost REQUIRED COMPONENTS system)
find_package(Threads REQUIRED)
find_package(nlohmann_json REQUIRED)

add_executable(socktest_backend
    main.cpp
    SocketManager.cpp
    WsSession.cpp
)

target_link_libraries(socktest_backend
    PRIVATE
    Boost::system
    Threads::Threads
    nlohmann_json::nlohmann_json
)

if(WIN32)
    target_link_libraries(socktest_backend PRIVATE ws2_32 mswsock)
endif()
