cmake_minimum_required(VERSION 3.20)

project(shifty_cpp_consumer LANGUAGES CXX)

find_package(Shifty CONFIG REQUIRED)

add_executable(shifty_cpp_consumer main.cpp)
target_link_libraries(shifty_cpp_consumer PRIVATE Shifty::shifty)
target_compile_features(shifty_cpp_consumer PRIVATE cxx_std_17)
