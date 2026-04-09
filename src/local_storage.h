#pragma once
#include "common.h"
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace LocalStorage {

struct FileEntry {
    std::string filename;
    std::vector<uint8_t> sha;     // 20-byte SHA1
    uint64_t timestamp = 0;
    uint64_t rawSize = 0;
    bool deleted = false;
    uint32_t rootId = 0;          // 0=remote/, 12=WinAppDataLocalLow, etc.
};

void Init(const std::string& baseRoot);
void InitApp(uint32_t accountId, uint32_t appId);
std::vector<FileEntry> GetFileList(uint32_t accountId, uint32_t appId);
std::optional<FileEntry> GetFileEntry(uint32_t accountId, uint32_t appId, const std::string& filename);
std::vector<uint8_t> ReadFile(uint32_t accountId, uint32_t appId, const std::string& filename);
bool WriteFile(uint32_t accountId, uint32_t appId, const std::string& filename, const uint8_t* data, size_t len);
bool DeleteFile(uint32_t accountId, uint32_t appId, const std::string& filename);
bool SetFileTimestamp(uint32_t accountId, uint32_t appId, const std::string& filename, uint64_t unixSeconds);
uint64_t GetChangeNumber(uint32_t accountId, uint32_t appId);
std::vector<uint8_t> SHA1(const uint8_t* data, size_t len);
std::string GetAppPath(uint32_t accountId, uint32_t appId);
std::vector<FileEntry> GetAutoCloudFileList(const std::string& steamPath, uint32_t appId);
void SaveRootTokens(uint32_t accountId, uint32_t appId, const std::unordered_set<std::string>& tokens);
std::unordered_set<std::string> LoadRootTokens(uint32_t accountId, uint32_t appId);

// Per-file token tracking: which root token each file was uploaded under.
// Prevents changelist from duplicating files across all tokens.
void SaveFileTokens(uint32_t accountId, uint32_t appId,
                    const std::unordered_map<std::string, std::string>& fileTokens);
std::unordered_map<std::string, std::string> LoadFileTokens(uint32_t accountId, uint32_t appId);

} // namespace LocalStorage
