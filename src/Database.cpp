#include "Database.hpp"

#define CHECK_DATABASE() \
    auto database = LUA->GetUserType<mongoc_database_t>(1, DatabaseMetaTableId); \
    if (database == nullptr) return 0; \

LUA_FUNCTION(destroy_database) {
    CHECK_DATABASE()

    mongoc_database_destroy(database);

    return 0;
}

LUA_FUNCTION(database_name) {
    CHECK_DATABASE()

    auto name = mongoc_database_get_name(database);

    LUA->PushString(name);

    return 1;
}

LUA_FUNCTION(database_copy) {
    CHECK_DATABASE()

    auto copy = mongoc_database_copy(database);

    LUA->PushUserType(copy, DatabaseMetaTableId);

    return 1;
}

LUA_FUNCTION(database_drop) {
    CHECK_DATABASE()

    SETUP_QUERY(error)

    bool success = mongoc_database_drop(database, &error);

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(database_command) {
    CHECK_DATABASE()

    CHECK_BSON(command)

    SETUP_QUERY(error, reply);

    bool success = mongoc_database_command_simple(database, command, nullptr, &reply, &error);

    CLEANUP_BSON(command)

    CLEANUP_QUERY(error, reply, !success)

    LUA->ReferencePush(BSONToLua(LUA, &reply));

    return 1;
}

LUA_FUNCTION(database_user_add) {
    CHECK_DATABASE()

    auto username = LUA->CheckString(2);
    auto password = LUA->CheckString(3);

    int rolesRef = INT_MIN, dataRef = INT_MIN;

    if (LUA->Top() == 5) {
        if (LUA->IsType(4, GarrysMod::Lua::Type::Table) && LUA->IsType(5, GarrysMod::Lua::Type::Table)) {
            dataRef = LUA->ReferenceCreate();
            rolesRef = LUA->ReferenceCreate();
        } else if (LUA->IsType(5, GarrysMod::Lua::Type::Table)) {
            dataRef = LUA->ReferenceCreate();
        } else {
            LUA->ThrowError("Incorrect number of arguments passed!");
        }
    } else if (LUA->Top() == 4 && LUA->IsType(4, GarrysMod::Lua::Type::Table)) {
        rolesRef = LUA->ReferenceCreate();
    }

    bson_t* roles;
    if (rolesRef != INT_MIN) {
        roles = LuaToBSON(LUA, rolesRef);
        LUA->ReferenceFree(rolesRef);
    }

    bson_t* data;
    if (dataRef != INT_MIN) {
        data = LuaToBSON(LUA, dataRef);
    }

    bson_error_t error;
    bool success = mongoc_database_add_user(database, username, password, roles, data, &error);

    if (rolesRef != INT_MIN) {
        LUA->ReferenceFree(rolesRef);
        bson_destroy(roles);
    }

    if (dataRef != INT_MIN) {
        LUA->ReferenceFree(dataRef);
        bson_destroy(data);
    }

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(database_user_remove) {
    CHECK_DATABASE()

    auto username = LUA->CheckString(2);

    SETUP_QUERY(error)

    bool success = mongoc_database_remove_user(database, username, &error);

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(database_collection_exists) {
    CHECK_DATABASE()

    auto name = LUA->CheckString(2);

    SETUP_QUERY(error)

    bool exists = mongoc_database_has_collection(database, name, &error);

    CLEANUP_QUERY(error, error.code != 0)

    LUA->PushBool(exists);

    return 1;
}

LUA_FUNCTION(database_collection_get) {
    CHECK_DATABASE()

    auto name = LUA->CheckString(2);

    auto collection = mongoc_database_get_collection(database, name);

    LUA->PushUserType(collection, CollectionMetaTableId);

    return 1;
}

LUA_FUNCTION(database_collection_create) {
    CHECK_DATABASE()

    auto name = LUA->CheckString(2);

    CHECK_BSON(options)

    SETUP_QUERY(error)

    auto collection = mongoc_database_create_collection(database, name, options, &error);

    CLEANUP_BSON(options)

    CLEANUP_QUERY(error, collection == nullptr);

    LUA->PushUserType(collection, CollectionMetaTableId);

    return 1;
}
