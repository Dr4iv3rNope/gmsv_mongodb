#include "Collection.hpp"

#define CHECK_COLLECTION() \
        auto collection = LUA->GetUserType<mongoc_collection_t>(1, CollectionMetaTableId); \
        if (collection == nullptr) return 0; \

LUA_FUNCTION(destroy_collection) {
    CHECK_COLLECTION()

    mongoc_collection_destroy(collection);

    return 0;
}

LUA_FUNCTION(collection_command) {
    CHECK_COLLECTION()

    CHECK_BSON(command, opts)

    SETUP_QUERY(error, reply)

    bool success = mongoc_collection_command_with_opts(collection, command, nullptr, opts, &reply, &error);

    CLEANUP_BSON(command, opts)

    CLEANUP_QUERY(error, reply, !success)

    LUA->ReferencePush(BSONToLua(LUA, &reply));

    return 1;
}

LUA_FUNCTION(collection_name) {
    CHECK_COLLECTION()

    LUA->PushString(mongoc_collection_get_name(collection));

    return 1;
}

LUA_FUNCTION(collection_count) {
    CHECK_COLLECTION()

    CHECK_BSON(filter, opts)

    SETUP_QUERY(error)

    int64_t count = mongoc_collection_count_documents(collection, filter, opts, nullptr, nullptr, &error);

    CLEANUP_BSON(filter, opts)

    CLEANUP_QUERY(error, count == -1)

    LUA->PushNumber((double)count);

    return 1;
}

LUA_FUNCTION(collection_find) {
    CHECK_COLLECTION()

    CHECK_BSON(filter, opts)

    auto cursor = mongoc_collection_find_with_opts(collection, filter, opts, mongoc_read_prefs_new(MONGOC_READ_PRIMARY));

    CLEANUP_BSON(filter, opts)

    LUA->ReferencePush(CreateLuaTableFromCursor(LUA, cursor));

    return 1;
}

LUA_FUNCTION(collection_find_and_modify) {
    CHECK_COLLECTION()

    CHECK_BSON(query)

    auto opts = LuaToFindAndModifyOpts(LUA, 2);

    SETUP_QUERY(error, reply)

    bool success = mongoc_collection_find_and_modify_with_opts(collection, query, opts, &reply, &error);

    mongoc_find_and_modify_opts_destroy(opts);

    CLEANUP_BSON(query)

    CLEANUP_QUERY(error, reply, !success)

    LUA->ReferencePush(BSONToLua(LUA, &reply));

    return 1;
}

LUA_FUNCTION(collection_insert) {
    CHECK_COLLECTION()

    CHECK_BSON(document)

    SETUP_QUERY(error)

    bool success = mongoc_collection_insert(collection, MONGOC_INSERT_NONE, document, nullptr, &error);

    CLEANUP_BSON(document)

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(collection_insert_one) {
    CHECK_COLLECTION()

    CHECK_BSON(document, opts)

    SETUP_QUERY(error, reply)

    bool success = mongoc_collection_insert_one(collection, document, opts, &reply, &error);

    CLEANUP_BSON(document, opts)

    CLEANUP_QUERY(error, reply, !success)

    LUA->ReferencePush(BSONToLua(LUA, &reply));

    return 1;
}

LUA_FUNCTION(collection_replace_one) {
    CHECK_COLLECTION()

    CHECK_BSON(selector, replacement, opts)

    SETUP_QUERY(error, reply)

    bool success = mongoc_collection_replace_one(collection, selector, replacement, opts, &reply, &error);

    CLEANUP_BSON(selector, replacement, opts)

    CLEANUP_QUERY(error, reply, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(collection_remove) {
    CHECK_COLLECTION()

    CHECK_BSON(selector)

    SETUP_QUERY(error)

    bool success = mongoc_collection_remove(collection, MONGOC_REMOVE_NONE, selector, nullptr, &error);

    CLEANUP_BSON(selector)

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(collection_update) {
    CHECK_COLLECTION()

    LUA->CheckType(2, GarrysMod::Lua::Type::Table);
    LUA->CheckType(3, GarrysMod::Lua::Type::Table);

    CHECK_BSON(selector, update)

    SETUP_QUERY(error)

    bool success = mongoc_collection_update(collection, MONGOC_UPDATE_MULTI_UPDATE, selector, update, nullptr, &error);

    CLEANUP_BSON(selector, update)

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(collection_update_one) {
    CHECK_COLLECTION()

    CHECK_BSON(selector, update, opts)

    SETUP_QUERY(error, reply)

    bool success = mongoc_collection_update_one(collection, selector, update, opts, &reply, &error);

    CLEANUP_BSON(selector, update, opts)

    CLEANUP_QUERY(error, reply, !success)

    LUA->ReferencePush(BSONToLua(LUA, &reply));

    return 1;
}

LUA_FUNCTION(collection_bulk) {
    CHECK_COLLECTION()

    CHECK_BSON(opts)

    auto bulk = mongoc_collection_create_bulk_operation_with_opts(collection, opts);

    CLEANUP_BSON(opts)

    LUA->PushUserType(bulk, BulkMetaTableId);

    return 1;
}
