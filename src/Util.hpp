#ifndef GMSV_MONGODB_UTIL_HPP
#define GMSV_MONGODB_UTIL_HPP

#include "MongoDB.hpp"

int BSONToLua(GarrysMod::Lua::ILuaBase* LUA, const bson_t* bson);

const char* LuaToJSON(GarrysMod::Lua::ILuaBase* LUA, int ref);

bson_t* LuaToBSON(GarrysMod::Lua::ILuaBase* LUA, int ref);

int CreateLuaTableFromCursor(GarrysMod::Lua::ILuaBase* LUA, mongoc_cursor_t* cursor);

mongoc_find_and_modify_opts_t* LuaToFindAndModifyOpts(GarrysMod::Lua::ILuaBase* LUA, int ref);

#endif //GMSV_MONGODB_UTIL_HPP
