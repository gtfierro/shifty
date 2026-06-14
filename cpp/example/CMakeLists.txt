cmake_minimum_required(VERSION 3.20)
project(validate_223p LANGUAGES CXX)

find_package(Shifty CONFIG REQUIRED)

add_executable(validate_223p main.cpp)
target_link_libraries(validate_223p PRIVATE Shifty::shifty)
target_compile_features(validate_223p PRIVATE cxx_std_17)
