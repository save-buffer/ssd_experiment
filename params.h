#pragma once

struct SyncLevel 
{
    enum
    {
        NONE,
        DSYNC,
        DIRECT,
    };
};

constexpr int levels[] = { 0, O_DSYNC, O_DIRECT };
constexpr const char *strs[] = { "NONE", "DSYNC", "DIRECT" };

constexpr size_t file_size = 4ull * 1024ull * 1024ull * 1024ull; // 4 GB
constexpr size_t chunk_size = 1ull * 1024ull * 1024ull; // 1 MB
constexpr size_t max_in_flight = 2;
constexpr size_t align = 512;
constexpr int sync_level = SyncLevel::DIRECT;


constexpr int sync_flag = levels[sync_level];
constexpr const char *sync_str = strs[sync_level];

