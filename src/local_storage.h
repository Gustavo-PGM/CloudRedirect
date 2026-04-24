#pragma once
#include "common.h"
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace LocalStorage {

struct FileEntry {
    std::string filename;
    std::string sourcePath;          // Real filesystem source for AutoCloud bootstrap only
    std::string rootToken;           // Cloud root token captured/resolved for this file
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
bool WriteFileNoIncrement(uint32_t accountId, uint32_t appId, const std::string& filename, const uint8_t* data, size_t len);
bool DeleteFile(uint32_t accountId, uint32_t appId, const std::string& filename);
// Atomic rollback helper: while holding the storage mutex, compare the file's
// current bytes to expectedData; if they match, copy backupPath over localPath
// when hadOriginal is true, or remove localPath when hadOriginal is false.
// Returns true if the rollback action was taken, false if the file was
// modified concurrently or on IO failure.
bool RestoreFileIfUnchanged(uint32_t accountId, uint32_t appId,
                            const std::string& filename,
                            const std::vector<uint8_t>& expectedData,
                            const std::string& backupPath,
                            bool hadOriginal);
bool SetFileTimestamp(uint32_t accountId, uint32_t appId, const std::string& filename, uint64_t unixSeconds);
uint64_t GetChangeNumber(uint32_t accountId, uint32_t appId);
void SetChangeNumber(uint32_t accountId, uint32_t appId, uint64_t cn);
uint64_t IncrementChangeNumber(uint32_t accountId, uint32_t appId);
std::vector<uint8_t> SHA1(const uint8_t* data, size_t len);
std::string GetAppPath(uint32_t accountId, uint32_t appId);
std::vector<FileEntry> GetAutoCloudFileList(const std::string& steamPath, uint32_t accountId, uint32_t appId);
void SaveRootTokens(uint32_t accountId, uint32_t appId, const std::unordered_set<std::string>& tokens);
std::unordered_set<std::string> LoadRootTokens(uint32_t accountId, uint32_t appId);

// Per-file token tracking: which root token each file was uploaded under.
// Prevents changelist from duplicating files across all tokens.
void SaveFileTokens(uint32_t accountId, uint32_t appId,
                    const std::unordered_map<std::string, std::string>& fileTokens);
std::unordered_map<std::string, std::string> LoadFileTokens(uint32_t accountId, uint32_t appId);

#ifdef CLOUDREDIRECT_TESTING
bool TestResolveAutoCloudRootOverride(const std::string& root, const std::string& path,
                                      const std::string& overrideRoot,
                                      const std::string& useInstead,
                                      const std::string& addPath,
                                      const std::string& find,
                                      const std::string& replace,
                                      std::string& outRoot,
                                      std::string& outResolvedPath);
bool TestIsSafeAutoCloudRelativePath(const std::string& path);
bool TestParseMinimalAutoCloudKVFixture();
#endif

} // namespace LocalStorage
