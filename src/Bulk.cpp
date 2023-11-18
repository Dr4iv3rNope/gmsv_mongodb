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

LUA_FUNCTION(bulk_remove_many) {
    CHECK_BULK()

    CHECK_BSON(selector, opts)

    SETUP_QUERY(error)

    bool success = mongoc_bulk_operation_remove_many_with_opts(bulk, selector, opts, &error);

    CLEANUP_BSON(selector, opts)

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(bulk_remove_one) {
    CHECK_BULK()

    CHECK_BSON(selector, opts)

    SETUP_QUERY(error)

    bool success = mongoc_bulk_operation_remove_one_with_opts(bulk, selector, opts, &error);

    CLEANUP_BSON(selector, opts)

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(bulk_update) {
    CHECK_BULK()

    CHECK_BSON(selector, document)

    bool upsert = LUA->GetBool(3);

    mongoc_bulk_operation_update(bulk, selector, document, upsert);

    CLEANUP_BSON(selector, document)

    return 0;
}

LUA_FUNCTION(bulk_update_many) {
    CHECK_BULK()

    CHECK_BSON(selector, document, opts)

    SETUP_QUERY(error)

    bool success = mongoc_bulk_operation_update_many_with_opts(bulk, selector, document, opts, &error);

    CLEANUP_BSON(selector, document, opts)

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(bulk_update_one) {
    CHECK_BULK()

    CHECK_BSON(selector, document, opts)

    SETUP_QUERY(error)

    bool success = mongoc_bulk_operation_update_one_with_opts(bulk, selector, document, opts, &error);

    CLEANUP_BSON(selector, document, opts)

    CLEANUP_QUERY(error, !success)

    LUA->PushBool(success);

    return 1;
}

LUA_FUNCTION(bulk_replace_one) {
    CHECK_BULK()

    CHECK_BSON(selector, document, opts)

    SETUP_QUERY(error);

    bool success = mongoc_bulk_operation_replace_one_with_opts(bulk, selector, document, opts, &error);

    CLEANUP_BSON(selector, document, opts)

    CLEANUP_QUERY(error, !success);

    LUA->PushBool(success);

    return 1;
}

