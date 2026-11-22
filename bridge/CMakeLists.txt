cmake_minimum_required(VERSION 3.20)
project(PipvaneBridge VERSION 3.0.1 LANGUAGES CXX)

# Enforce C++17 Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Architecture Detection:
#  32-bit (Win32 / x86): Produces 'ipc_bridge_x86.dll' for MetaTrader 4
#  64-bit (x64)        : Produces 'PipvaneBridge.dll' for MetaTrader 5
if (CMAKE_SIZEOF_VOID_P EQUAL 4)
    set(BRIDGE_TARGET_NAME "ipc_bridge_x86")
    message(STATUS "Configuring 32-bit x86 target for MetaTrader 4: ${BRIDGE_TARGET_NAME}")
else()
    set(BRIDGE_TARGET_NAME "PipvaneBridge")
    message(STATUS "Configuring 64-bit x64 target for MetaTrader 5: ${BRIDGE_TARGET_NAME}")
endif()

# Define Shared Library Target
add_library(${BRIDGE_TARGET_NAME} SHARED
    ipc_bridge.cpp
    ipc_bridge.h
)

target_include_directories(${BRIDGE_TARGET_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Compiler & Linker Configuration
if (MSVC)
    target_compile_definitions(${BRIDGE_TARGET_NAME} PRIVATE
        WIN32_LEAN_AND_MEAN
        NOMINMAX
        _CRT_SECURE_NO_WARNINGS
        PIPVANE_BRIDGE_EXPORTS
        _WIN32_WINNT=0x0A00
        WINVER=0x0A00
    )

    # Security Hardening & Ruthless Optimization Flags:
    #  /O2        : Maximize execution speed
    #  /Oi        : Enable intrinsic functions
    #  /GL        : Whole program optimization
    #  /GS        : Buffer security check (Stack Canary)
    #  /guard:cf  : Control Flow Guard (CFG)
    #  /W3        : Warning Level 3
    #  /fp:fast   : Fast floating-point model
    #  /MP        : Multi-processor compilation
    target_compile_options(${BRIDGE_TARGET_NAME} PRIVATE
        /O2
        /Oi
        /GL
        /GS
        /guard:cf
        /W3
        /permissive-
        /fp:fast
        /MP
    )

    # Base Linker Options:
    #  /DYNAMICBASE    : Address Space Layout Randomization (ASLR)
    #  /NXCOMPAT       : Data Execution Prevention (DEP)
    #  /LTCG           : Link-Time Code Generation
    #  /guard:cf       : Control Flow Guard for Linker
    #  /OPT:REF        : Eliminate unreferenced functions
    #  /OPT:ICF        : Identical COMDAT folding
    #  /DEF            : Module definition file for exact unmangled MQL exports
    target_link_options(${BRIDGE_TARGET_NAME} PRIVATE
        /DYNAMICBASE
        /NXCOMPAT
        /LTCG
        /guard:cf
        /OPT:REF
        /OPT:ICF
    )

    if (CMAKE_SIZEOF_VOID_P EQUAL 4)
        # 32-bit x86 Specific Hardening & Architecture
        target_compile_options(${BRIDGE_TARGET_NAME} PRIVATE /arch:SSE2)
        target_link_options(${BRIDGE_TARGET_NAME} PRIVATE /SAFESEH:NO)
    else()
        # 64-bit x64 Specific Hardening
        target_link_options(${BRIDGE_TARGET_NAME} PRIVATE /HIGHENTROPYVA) # 64-bit ASLR
    endif()

    # Link strictly against Windows CNG and Kernel32 (NO ws2_32.lib network sockets)
    target_link_libraries(${BRIDGE_TARGET_NAME} PRIVATE
        bcrypt.lib
        kernel32.lib
    )
else()
    # Clang / GCC toolchain flags for non-MSVC cross-compilation
    target_compile_options(${BRIDGE_TARGET_NAME} PRIVATE
        -O3
        -fstack-protector-strong
        -Wall
        -Wextra
        -fPIC
    )
    if (CMAKE_SIZEOF_VOID_P EQUAL 4)
        target_compile_options(${BRIDGE_TARGET_NAME} PRIVATE -m32 -msse2)
        target_link_options(${BRIDGE_TARGET_NAME} PRIVATE -m32)
    endif()

    target_link_libraries(${BRIDGE_TARGET_NAME} PRIVATE
        bcrypt
        kernel32
    )
endif()

# Set output binary properties
set_target_properties(${BRIDGE_TARGET_NAME} PROPERTIES
    OUTPUT_NAME "${BRIDGE_TARGET_NAME}"
    PREFIX ""
)
