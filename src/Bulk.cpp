#include "Bulk.hpp"

#define CHECK_BULK() \
    auto bulk = LUA->GetUserType<mongoc_bulk_operation_t>(1, BulkMetaTableId); \
    if (bulk == nullptr) return 0;

LUA_FUNCTION(destroy_bulk) {
    CHECK_BULK()

    mongoc_bulk_operation_destroy(bulk);

    return 0;
}

LUA_FUNCTION(bulk_execute) {
    CHECK_BULK()

    SETUP_QUERY(error, reply)

    bool success = mongoc_bulk_operation_execute(bulk, &reply, &error);

    CLEANUP_QUERY(error, reply, !success)

    LUA->ReferencePush(BSONToLua(LUA, &reply));

    return 1;
}

LUA_FUNCTION(bulk_insert) {
    CHECK_BULK()

    CHECK_BSON(query, opts)

    SETUP_QUERY(error)

    bool success = mongoc_bulk_operation_insert_with_opts(bulk, query, opts, &error);

    CLEANUP_BSON(query, opts)

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(bulk_remove) {
    CHECK_BULK()

    CHECK_BSON(selector)

    SETUP_QUERY(error)

    mongoc_bulk_operation_remove(bulk, selector);

    CLEANUP_BSON(selector)

    return 0;
}

    return 1;
}

LUA_FUNCTION(bulk_update) {
    CHECK_BULK()

    LUA->CheckType(2, GarrysMod::Lua::Type::Table);
    LUA->CheckType(3, GarrysMod::Lua::Type::Table);

    CHECK_BSON(selector, document, opts)

    bson_error_t error;
    bool success = mongoc_bulk_operation_update_one_with_opts(bulk, selector, document, opts, &error);

    CLEANUP_BSON(selector, document, opts)

    if (!success) {
        LUA->ThrowError(error.message);
        return 0;
    }

    return 1;
}

LUA_FUNCTION(bulk_update_many) {
    CHECK_BULK()

    return 1;
}

LUA_FUNCTION(bulk_replace) {
    CHECK_BULK()

    LUA->CheckType(2, GarrysMod::Lua::Type::Table);
    LUA->CheckType(3, GarrysMod::Lua::Type::Table);

    CHECK_BSON(selector, document, opts)

    bson_error_t error;
    bool success = mongoc_bulk_operation_replace_one_with_opts(bulk, selector, document, opts, &error);

    CLEANUP_BSON(selector, document, opts)

    if (!success) {
        LUA->ThrowError(error.message);
        return 0;
    }

    return 1;
}

