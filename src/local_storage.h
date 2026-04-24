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

// Delete tombstones: persistent record of filenames the user deleted locally
// whose cloud Remove has not yet been confirmed. SyncFromCloud consults this
// map and refuses to redownload a tombstoned filename, preventing resurrection
// after a Delete work item fails its transfer retries and drain requeues.
//
// Each tombstone is stored with the local change number at delete time. When
// SyncFromCloud sees a newer cloud CN, the caller can compare it against the
// tombstone CN and suppress the tombstone if cloud writes happened after the
// local delete (i.e. another machine re-created the file). This preserves
// Steam's "newer CN wins" semantics across machines while still blocking
// same-machine resurrection from a failed Remove.
//
// On-disk format (deleted.dat, per app):
//   filename\tcn\n
// Legacy lines without a tab are loaded with cn=0 (treated as "always wins
// against any newer cloud CN that's ever existed"), matching pre-CN behavior.
//
// Tombstones are cleared on successful cloud Remove (worker) or when the user
// re-creates the file via StoreBlob. The latter also covers user-initiated
// writes that resurface the same filename after a stuck Delete.
void MarkDeleted(uint32_t accountId, uint32_t appId, const std::string& filename,
                 uint64_t cnAtDelete);
void ClearDeleted(uint32_t accountId, uint32_t appId, const std::string& filename);
bool IsDeleted(uint32_t accountId, uint32_t appId, const std::string& filename);
std::unordered_map<std::string, uint64_t> LoadDeleted(uint32_t accountId,
                                                      uint32_t appId);

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
// Exercises platforms-mask filter and exclude-list filter on a savefiles rule.
// Returns true when the Windows rule is kept (platforms=Windows), the Linux-only
// rule is dropped, and the exclude pattern "*.log" filters leaves correctly.
bool TestAutoCloudPlatformAndExcludeFilters();
#endif

} // namespace LocalStorage
