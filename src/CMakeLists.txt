cmake_minimum_required (VERSION 3.13)

find_package(mongoc-1.0 CONFIG REQUIRED)

add_library(mongodb SHARED
        Client.hpp
        Client.cpp
        Util.cpp
        Util.hpp
        Database.cpp
        Database.hpp
        MongoDB.cpp
        MongoDB.hpp
        Collection.cpp
        Collection.hpp
        Bulk.cpp Bulk.hpp Macros.hpp ObjectID.cpp ObjectID.hpp)

if(${WIN32})
    target_link_options(mongodb PRIVATE "/NODEFAULTLIB:LIBCMT")

    target_link_options(mongodb PRIVATE "/NODEFAULTLIB:LIBCMTD")

    target_link_libraries(mongodb PRIVATE ws2_32 wsock32)
endif()

target_link_libraries(mongodb PRIVATE mongo::mongoc_static)

target_link_libraries(mongodb PRIVATE gmod-module-base)

set_gmod_suffix_prefix(mongodb)