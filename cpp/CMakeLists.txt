cmake_minimum_required(VERSION 3.20)

file(
    STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/../Cargo.toml"
    SHIFTY_VERSION_LINE
    REGEX "^version = \"[0-9]+\\.[0-9]+\\.[0-9]+\"$")
string(
    REGEX REPLACE
    "^version = \"([0-9]+\\.[0-9]+\\.[0-9]+)\"$"
    "\\1"
    SHIFTY_VERSION
    "${SHIFTY_VERSION_LINE}")

project(
    shifty_cpp
    VERSION "${SHIFTY_VERSION}"
    DESCRIPTION "C++ SDK for the shifty RDF, SPARQL, and SHACL engine"
    LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_package(Threads REQUIRED)

option(SHIFTY_CPP_BUILD_TESTS "Build the C++ SDK integration tests" ON)

set(SHIFTY_MANIFEST "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml")
set(SHIFTY_CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/cargo-target")
file(
    GLOB_RECURSE SHIFTY_RUST_SOURCES
    CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/../crates/*/src/*.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/../crates/*/Cargo.toml")

if(WIN32)
    set(SHIFTY_LIBRARY_FILE "shifty_cpp.lib")
else()
    set(SHIFTY_LIBRARY_FILE "libshifty_cpp.a")
endif()

set(
    SHIFTY_RUST_LIBRARY
    "${SHIFTY_CARGO_TARGET_DIR}/release/${SHIFTY_LIBRARY_FILE}")

add_custom_command(
    OUTPUT "${SHIFTY_RUST_LIBRARY}"
    COMMAND
        "${CMAKE_COMMAND}" -E env
        "CARGO_TARGET_DIR=${SHIFTY_CARGO_TARGET_DIR}"
        "CARGO_PROFILE_RELEASE_DEBUG=0"
        "CARGO_PROFILE_RELEASE_STRIP=symbols"
        cargo build --manifest-path "${SHIFTY_MANIFEST}" --release
    DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml"
        "${CMAKE_CURRENT_SOURCE_DIR}/../Cargo.toml"
        "${CMAKE_CURRENT_SOURCE_DIR}/../Cargo.lock"
        ${SHIFTY_RUST_SOURCES}
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Building the shifty Rust static library"
    VERBATIM)

add_custom_target(shifty_rust_build DEPENDS "${SHIFTY_RUST_LIBRARY}")

add_library(shifty_rust STATIC IMPORTED GLOBAL)
set_target_properties(
    shifty_rust
    PROPERTIES IMPORTED_LOCATION "${SHIFTY_RUST_LIBRARY}")
add_dependencies(shifty_rust shifty_rust_build)

add_library(shifty INTERFACE)
add_library(Shifty::shifty ALIAS shifty)
target_compile_features(shifty INTERFACE cxx_std_17)
target_include_directories(
    shifty
    INTERFACE
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
target_link_libraries(shifty INTERFACE shifty_rust Threads::Threads ${CMAKE_DL_LIBS})

if(UNIX AND NOT APPLE)
    target_link_libraries(shifty INTERFACE m)
    set(SHIFTY_EXTRA_SYSTEM_LIBS "m")
else()
    set(SHIFTY_EXTRA_SYSTEM_LIBS "")
endif()

if(SHIFTY_CPP_BUILD_TESTS)
    enable_testing()
    add_executable(shifty_cpp_smoke tests/smoke.cpp)
    target_link_libraries(shifty_cpp_smoke PRIVATE Shifty::shifty)
    if(MSVC)
        target_compile_options(shifty_cpp_smoke PRIVATE /W4 /permissive-)
    else()
        target_compile_options(
            shifty_cpp_smoke
            PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
    endif()
    add_test(NAME shifty_cpp_smoke COMMAND shifty_cpp_smoke)
endif()

install(
    DIRECTORY include/
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(
    FILES "${SHIFTY_RUST_LIBRARY}"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}")

configure_package_config_file(
    cmake/ShiftyConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/ShiftyConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Shifty")
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/ShiftyConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY SameMajorVersion)
install(
    FILES
        "${CMAKE_CURRENT_BINARY_DIR}/ShiftyConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/ShiftyConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/Shifty")
