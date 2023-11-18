#ifndef GMSV_MONGODB_BULK_HPP
#define GMSV_MONGODB_BULK_HPP

#include "MongoDB.hpp"

int destroy_bulk(lua_State* L);

int bulk_execute(lua_State* L);

int bulk_insert(lua_State* L);

int bulk_remove(lua_State* L);

int bulk_remove_many(lua_State* L);

int bulk_remove_one(lua_State* L);

int bulk_update(lua_State* L);

int bulk_update_many(lua_State* L);

int bulk_update_one(lua_State* L);

int bulk_replace_one(lua_State* L);

extern int BulkMetaTableId;

#endif //GMSV_MONGODB_BULK_HPP
