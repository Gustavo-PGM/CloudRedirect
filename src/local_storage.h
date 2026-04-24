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

// Remove empty cache subdirectories above files that were just deleted,
// bounded by the app root. Serialized against WriteFileNoIncrement /
// RestoreFileIfUnchanged / DeleteFile under the storage mutex so concurrent
// writers never observe a create_directories() + AtomicWriteBinary() pair
// where the parent dir disappears between the two calls. Best-effort: any
// filesystem error silently stops the walk. Caller passes the absolute
// parent directory of the file(s) that were just removed. For batch
// deletions, passing multiple paths avoids one lock acquisition per file
// and processes deepest-first so upward walks can cascade in one pass.
void CleanupEmptyCacheDirs(uint32_t accountId, uint32_t appId,
                           std::vector<std::string> startDirs);
uint64_t GetChangeNumber(uint32_t accountId, uint32_t appId);
void SetChangeNumber(uint32_t accountId, uint32_t appId, uint64_t cn);
uint64_t IncrementChangeNumber(uint32_t accountId, uint32_t appId);
std::vector<uint8_t> SHA1(const uint8_t* data, size_t len);
std::string GetAppPath(uint32_t accountId, uint32_t appId);
// Result of a bounded AutoCloud save-rule scan.
// - files: rule-matched files observed before any termination condition.
// - scanLimitHit: the resource cap (kMaxAutoCloudScanFiles /
//   kMaxAutoCloudScanMillis) was reached; `files` is a truncated prefix of
//   what the filesystem contains. Callers MUST NOT commit to an import
//   based on this — they can't tell whether an unobserved file is a
//   foreign-app pollution sentinel or a legitimate save. They also MUST
//   NOT clear any canonical-token cache: a partial scan is not corruption
//   evidence, and discarding a previously-populated cache would break
//   incoming ChangelistGet canonicalization for files that were scanned
//   successfully on a prior session.
// - hasRootCollision: two rules resolved to the same cloud path under
//   different root tokens. `files` is cleared. Import must abort.
struct AutoCloudScanResult {
    std::vector<FileEntry> files;
    bool scanLimitHit = false;
    bool hasRootCollision = false;
};

AutoCloudScanResult GetAutoCloudFileList(const std::string& steamPath,
                                         uint32_t accountId, uint32_t appId);
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
//
// The CN alone does not safely distinguish "another machine wrote after my
// delete" (tombstone should clear) from "my own later batches advanced cloud
// CN while my Delete was poisoned in flight" (tombstone must stay). We also
// stamp the tombstone with a wall-clock creation time and let the cloud-side
// sync check the blob's modified time against it — the blob mtime only
// advances when someone actually wrote to the file, which is the real signal.
// Legacy tombstones with createTimeUnix=0 fall back to CN-only comparison.
struct TombstoneInfo {
    uint64_t cn = 0;               // local CN at MarkDeleted time
    uint64_t createTimeUnix = 0;   // wall-clock unix seconds; 0 means "legacy/unknown"
};

void MarkDeleted(uint32_t accountId, uint32_t appId, const std::string& filename,
                 uint64_t cnAtDelete);
void ClearDeleted(uint32_t accountId, uint32_t appId, const std::string& filename);
bool IsDeleted(uint32_t accountId, uint32_t appId, const std::string& filename);
std::unordered_map<std::string, TombstoneInfo> LoadDeleted(uint32_t accountId,
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
// Exercises the VDF `siblings` splitter and path-safety filter.
// Returns the cleaned extension-token list (tokens are stored without
// leading dots, mirroring Steam's compose-time format). See
// ParseAutoCloudSiblings doc for the rejection rules.
std::vector<std::string> TestParseAutoCloudSiblings(const std::string& raw);
// Exercises platforms-mask filter and exclude-list filter on a savefiles rule.
// Returns true when the Windows rule is kept (platforms=Windows), the Linux-only
// rule is dropped, and the exclude pattern "*.log" filters leaves correctly.
bool TestAutoCloudPlatformAndExcludeFilters();
#endif

} // namespace LocalStorage
