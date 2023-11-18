#include "Util.hpp"

#define LUA_READ_FIELD(index, fieldName, fieldType, code) \
    LUA->GetField(ref, fieldName); \
    if (LUA->IsType(index, GarrysMod::Lua::Type::fieldType)) \
    code; \
    LUA->Pop();

#define TRY_ADD_FIND_AND_MODIFY_PARAM(paramName) \
    LUA_READ_FIELD(ref, #paramName, Table, { \
        auto bson = LuaToBSON(LUA, -1); \
        mongoc_find_and_modify_opts_set_##paramName(opts, bson); \
        CLEANUP_BSON(bson) \
    }) \

bool IsLuaTableSequential(GarrysMod::Lua::ILuaBase* LUA, int ref) {
    return LUA->ObjLen(ref) != 0;
}

void WriteLuaValueToBSON(bson_t* dest, GarrysMod::Lua::ILuaBase* LUA) {
    auto key = LUA->GetString(-2);

    switch (LUA->GetType(-1))
    {
    case GarrysMod::Lua::Type::Bool:
        bson_append_bool(dest, key, -1, LUA->GetBool(-1));
        break;

    case GarrysMod::Lua::Type::Number:
        bson_append_double(dest, key, -1, LUA->GetNumber(-1));
        break;

    case GarrysMod::Lua::Type::String:
        bson_append_utf8(dest, key, -1, LUA->GetString(-1), -1);
        break;

    case GarrysMod::Lua::Type::Table:
    {
        auto bson = LuaToBSON(LUA, -1);

        if (IsLuaTableSequential(LUA, -1)) {
            bson_append_array(dest, key, -1, bson);
        } else {
            bson_append_document(dest, key, -1, bson);
        }

        bson_destroy(bson);
        break;
    }

    default:
        break;
    }
}

void WriteLuaTableToBSON(bson_t* dest, GarrysMod::Lua::ILuaBase* LUA, int ref) {
    while (LUA->Next(ref) != 0) {
        WriteLuaValueToBSON(dest, LUA);

        LUA->Pop();
    }
}

bson_t* LuaToBSON(GarrysMod::Lua::ILuaBase* LUA, int ref) {
    auto bson = bson_new();

    WriteLuaTableToBSON(bson, LUA, ref);

    return bson;
}

int BSONToLua(GarrysMod::Lua::ILuaBase* LUA, const bson_t* bson) {
    bson_iter_t iter;

    LUA->CreateTable();

    if (bson_iter_init(&iter, bson)) {
        while (bson_iter_next(&iter)) {
            auto type = bson_iter_type(&iter);

            switch (type) {
                case BSON_TYPE_DOUBLE:
                    LUA->PushNumber(bson_iter_as_double(&iter));
                    break;
                case BSON_TYPE_INT32:
                case BSON_TYPE_INT64:
                    LUA->PushNumber((double) bson_iter_as_int64(&iter));
                    break;
                case BSON_TYPE_BOOL:
                    LUA->PushBool(bson_iter_as_bool(&iter));
                    break;
                case BSON_TYPE_UTF8:
                    LUA->PushString(bson_iter_utf8(&iter, nullptr));
                    break;
                case BSON_TYPE_DATE_TIME:
                    LUA->PushNumber((double) bson_iter_date_time(&iter));
                    break;
                case BSON_TYPE_REGEX:
                    LUA->PushString(bson_iter_regex(&iter, nullptr));
                    break;
                case BSON_TYPE_CODE:
                    LUA->PushString(bson_iter_code(&iter, nullptr));
                    break;
                case BSON_TYPE_TIMESTAMP: {
                    uint32_t t;
                    bson_iter_timestamp(&iter, &t, nullptr);
                    LUA->PushNumber(t);
                    break;
                }
                case BSON_TYPE_OID: {
                    const bson_oid_t *oid = bson_iter_oid(&iter);

                    LUA->PushUserType((void *) oid, ObjectIDMetaTableId);
                    break;
                }
                case BSON_TYPE_DOCUMENT: {
                    uint32_t len;
                    const uint8_t *data;
                    bson_t b;
                    bson_iter_document(&iter, &len, &data);
                    bson_init_static(&b, data, (size_t) len);
                    LUA->ReferencePush(BSONToLua(LUA, &b));
                    bson_destroy(&b);
                    break;
                }
                case BSON_TYPE_ARRAY: {
                    uint32_t len;
                    const uint8_t *data;
                    bson_t b;
                    bson_iter_array(&iter, &len, &data);
                    bson_init_static(&b, data, (size_t) len);
                    LUA->ReferencePush(BSONToLua(LUA, &b));
                    bson_destroy(&b);
                    break;
                }
                case BSON_TYPE_NULL:
                    LUA->PushNil();
                    break;
                default:
                    continue;
            }

            LUA->SetField(-2, bson_iter_key(&iter));
        }
    }

    return LUA->ReferenceCreate();
}

int CreateLuaTableFromCursor(GarrysMod::Lua::ILuaBase* LUA, mongoc_cursor_t* cursor) {
    LUA->CreateTable();

    int table = LUA->ReferenceCreate();

    const bson_t * bson;
    for (int i = 0; mongoc_cursor_next(cursor, &bson); ++i) {
        LUA->ReferencePush(table);
        LUA->PushNumber(i + 1);
        LUA->ReferencePush(BSONToLua(LUA, bson));
        LUA->SetTable(-3);
    }

    mongoc_cursor_destroy(cursor);

    return table;
}

mongoc_find_and_modify_opts_t* LuaToFindAndModifyOpts(GarrysMod::Lua::ILuaBase* LUA, int ref) {
    auto opts = mongoc_find_and_modify_opts_new();

    if (LUA->IsType(ref, GarrysMod::Lua::Type::Table)) {
        TRY_ADD_FIND_AND_MODIFY_PARAM(fields)
        TRY_ADD_FIND_AND_MODIFY_PARAM(sort)
        TRY_ADD_FIND_AND_MODIFY_PARAM(update)

        // flags
        LUA_READ_FIELD(ref, "flags", Table, {
            int flags = MONGOC_FIND_AND_MODIFY_NONE;

            LUA_READ_FIELD(-1, "remove",     Bool, flags |= (int)MONGOC_FIND_AND_MODIFY_REMOVE)
            LUA_READ_FIELD(-1, "upsert",     Bool, flags |= (int)MONGOC_FIND_AND_MODIFY_UPSERT)
            LUA_READ_FIELD(-1, "return_new", Bool, flags |= (int)MONGOC_FIND_AND_MODIFY_RETURN_NEW)

            mongoc_find_and_modify_opts_set_flags(opts, (mongoc_find_and_modify_flags_t)flags);
        })
    }

    return opts;
}
