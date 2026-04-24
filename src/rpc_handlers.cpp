#include "rpc_handlers.h"
#include "cloud_intercept.h"
#include "local_storage.h"
#include "http_server.h"
#include "http_util.h"
#include "cloud_storage.h"
#include "file_util.h"
#include "vdf.h"
#include "log.h"
#include "json.h"
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <sstream>

namespace CloudIntercept {

bool RestorePlaytimeState(uint32_t appId, uint64_t playtime, uint64_t playtime2wks);
bool RestoreLastPlayedState(uint32_t appId, uint64_t lastPlayed);

static void RestoreInMemoryPlaytimeMetadata(uint32_t appId, uint64_t lastPlayed,
                                            uint64_t playtime, uint64_t playtime2wks) {
    RestoreLastPlayedState(appId, lastPlayed);
    RestorePlaytimeState(appId, playtime, playtime2wks);
}


// per-app upload batch tracking
static std::atomic<uint64_t> g_nextBatchId{1};

static uint64_t MakeAppAccountKey(uint32_t accountId, uint32_t appId) {
    return (static_cast<uint64_t>(accountId) << 32) | appId;
}

static bool RequireAccountId(const char* op, uint32_t appId, uint32_t& accountId) {
    constexpr ULONGLONG timeoutMs = 5000;
    constexpr DWORD sleepMs = 10;

    ULONGLONG deadline = GetTickCount64() + timeoutMs;
    do {
        accountId = GetAccountId();
        if (accountId != 0) return true;
        Sleep(sleepMs);
    } while (GetTickCount64() < deadline);

    LOG("[NS] %s app=%u timed out waiting for Steam account ID", op, appId);
    return false;
}

static bool IsInternalMetadataFile(std::string_view cleanName) {
    return cleanName == kPlaytimeMetadataPath || cleanName == kStatsMetadataPath ||
        cleanName == kLegacyPlaytimeMetadataPath || cleanName == kLegacyStatsMetadataPath;
}

static void InvalidateTokenCaches(uint32_t accountId, uint32_t appId);
static bool TryInvalidateTokenCachesForGeneration(uint32_t accountId, uint32_t appId,
                                                  uint64_t generation,
                                                  uint64_t& nextGeneration);

// Mutex map for AutoCloud state. Each guards a distinct piece of data, and
// multi-mutex sequences below take their inner locks in sequential scopes,
// never nested, so the single nested pair is:
//   g_autoCloudImportMutex  ->  <any inner token-cache mutex>
// Acquire order is stable across all call sites (see InvalidateTokenCaches
// and TryInvalidateTokenCachesForGeneration): token cache, root tokens,
// file tokens, batch tokens, bootstrap set. The generation counter in
// g_autoCloudCanonicalTokenGeneration is the actual race guard for the
// import -> publish handoff; no dedicated publish mutex is needed.
static std::mutex g_autoCloudTokenCacheMutex;
static std::unordered_map<uint64_t, std::unordered_map<std::string, std::string>> g_autoCloudCanonicalTokenCache;
static std::unordered_map<uint64_t, uint64_t> g_autoCloudCanonicalTokenGeneration;
static std::mutex g_autoCloudImportMutex;
static std::mutex g_autoCloudBootstrapMutex;
static std::condition_variable g_autoCloudBootstrapCV;
static std::unordered_set<uint64_t> g_autoCloudBootstrapAttemptedApps;
static std::unordered_set<uint64_t> g_autoCloudBootstrapActiveApps;
static std::vector<std::future<void>> g_autoCloudBootstrapFutures;
static bool g_autoCloudBootstrapShuttingDown = false;

static void CacheAutoCloudCanonicalTokens(uint32_t accountId, uint32_t appId,
                                          const std::vector<LocalStorage::FileEntry>& candidates,
                                          uint64_t generation) {
    std::unordered_map<std::string, std::string> tokens;
    for (const auto& fe : candidates) {
        if (!fe.filename.empty()) tokens.emplace(fe.filename, fe.rootToken);
    }
    std::lock_guard<std::mutex> lock(g_autoCloudTokenCacheMutex);
    uint64_t key = MakeAppAccountKey(accountId, appId);
    if (g_autoCloudCanonicalTokenGeneration[key] != generation) return;
    g_autoCloudCanonicalTokenCache[key] = std::move(tokens);
}

static void ClearAutoCloudCanonicalTokens(uint32_t accountId, uint32_t appId,
                                          uint64_t generation) {
    std::lock_guard<std::mutex> lock(g_autoCloudTokenCacheMutex);
    uint64_t key = MakeAppAccountKey(accountId, appId);
    if (g_autoCloudCanonicalTokenGeneration[key] != generation) return;
    g_autoCloudCanonicalTokenCache.erase(key);
}

static uint64_t GetAutoCloudCanonicalTokenGeneration(uint32_t accountId, uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_autoCloudTokenCacheMutex);
    return g_autoCloudCanonicalTokenGeneration[MakeAppAccountKey(accountId, appId)];
}

static bool TryBeginAutoCloudBootstrap(uint32_t accountId, uint32_t appId) {
    uint64_t appKey = MakeAppAccountKey(accountId, appId);
    std::lock_guard<std::mutex> lock(g_autoCloudBootstrapMutex);
    for (auto it = g_autoCloudBootstrapFutures.begin(); it != g_autoCloudBootstrapFutures.end(); ) {
        if (it->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            try { it->get(); } catch (...) {}
            it = g_autoCloudBootstrapFutures.erase(it);
        } else {
            ++it;
        }
    }
    if (g_autoCloudBootstrapShuttingDown || g_autoCloudBootstrapAttemptedApps.count(appKey) ||
        g_autoCloudBootstrapActiveApps.count(appKey)) {
        return false;
    }
    g_autoCloudBootstrapActiveApps.insert(appKey);
    return true;
}

static void FinishAutoCloudBootstrap(uint32_t accountId, uint32_t appId,
                                     bool markAttempted, uint64_t generation) {
    uint64_t appKey = MakeAppAccountKey(accountId, appId);
    bool generationCurrent = GetAutoCloudCanonicalTokenGeneration(accountId, appId) == generation;
    std::lock_guard<std::mutex> lock(g_autoCloudBootstrapMutex);
    g_autoCloudBootstrapActiveApps.erase(appKey);
    if (markAttempted && generationCurrent) g_autoCloudBootstrapAttemptedApps.insert(appKey);
    g_autoCloudBootstrapCV.notify_all();
}

static void WaitForAutoCloudBootstrap(uint32_t accountId, uint32_t appId) {
    uint64_t appKey = MakeAppAccountKey(accountId, appId);
    std::unique_lock<std::mutex> lock(g_autoCloudBootstrapMutex);
    g_autoCloudBootstrapCV.wait(lock, [&] {
        return !g_autoCloudBootstrapActiveApps.count(appKey);
    });
}

static bool IsAutoCloudBootstrapActive(uint32_t accountId, uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_autoCloudBootstrapMutex);
    return g_autoCloudBootstrapActiveApps.count(MakeAppAccountKey(accountId, appId)) != 0;
}

// Cooperative shutdown probe for BootstrapAutoCloudFilesWorker. Checked at
// safe abort points BEFORE any blob writes have been persisted — once the
// worker is past the local-write region it commits fully (including CN
// publish) so on-disk state stays consistent.
static bool IsAutoCloudBootstrapShuttingDown() {
    std::lock_guard<std::mutex> lock(g_autoCloudBootstrapMutex);
    return g_autoCloudBootstrapShuttingDown;
}

static bool LooksLikeForeignAppPollution(const std::string& filename, uint32_t appId) {
    size_t pos = filename.find_first_of("/\\");
    if (pos != std::string::npos && pos >= 3 && pos <= 10) {
        const std::string prefix = filename.substr(0, pos);
        if (std::all_of(prefix.begin(), prefix.end(), [](unsigned char c) { return std::isdigit(c); })) {
            try {
                uint32_t embeddedAppId = static_cast<uint32_t>(std::stoul(prefix));
                if (embeddedAppId != 0 && embeddedAppId != appId) return true;
            } catch (...) {
            }
        }
    }

    size_t underscore = filename.find("_%");
    if (underscore != std::string::npos && underscore >= 3 && underscore <= 10) {
        const std::string prefix = filename.substr(0, underscore);
        if (std::all_of(prefix.begin(), prefix.end(), [](unsigned char c) { return std::isdigit(c); })) {
            try {
                uint32_t embeddedAppId = static_cast<uint32_t>(std::stoul(prefix));
                if (embeddedAppId != 0 && embeddedAppId != appId) return true;
            } catch (...) {
            }
        }
    }

    return false;
}

static constexpr uint64_t kMaxAutoCloudImportBytes = 128ULL * 1024 * 1024;

static std::vector<uint8_t> ReadWholeFile(const std::string& path, bool& ok) {
    ok = false;
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) return {};
    auto size = f.tellg();
    if (size < 0) return {};
    if (static_cast<uint64_t>(size) > kMaxAutoCloudImportBytes) {
        LOG("[AutoCloudImport] Skipping oversized source: %s (%llu bytes)",
            path.c_str(), static_cast<unsigned long long>(size));
        return {};
    }
    f.seekg(0, std::ios::beg);
    std::vector<uint8_t> data(static_cast<size_t>(size));
    if (!data.empty() && !f.read(reinterpret_cast<char*>(data.data()), size)) return {};
    ok = true;
    return data;
}

static bool HasPersistedLocalCloudHistory(uint32_t accountId, uint32_t appId) {
    return std::filesystem::exists(std::filesystem::path(LocalStorage::GetAppPath(accountId, appId)) / "cn.dat");
}

static void BootstrapAutoCloudFilesWorker(uint32_t accountId, uint32_t appId,
                                          uint64_t cacheGeneration) {
    auto finish = [&](bool markAttempted, uint64_t generation) {
        FinishAutoCloudBootstrap(accountId, appId, markAttempted, generation);
    };

    // Abort early if shutdown was signaled before the scan ran. The scan can
    // walk thousands of files and has a 5-second wall budget; refusing here
    // lets the process exit promptly when a user closes Steam right after
    // launch. markAttempted=false so a subsequent session can retry if the
    // process is somehow re-initialized without reloading the DLL.
    if (IsAutoCloudBootstrapShuttingDown()) {
        LOG("[AutoCloudImport] Aborting bootstrap for app %u — shutdown in progress", appId);
        ClearAutoCloudCanonicalTokens(accountId, appId, cacheGeneration);
        finish(false, cacheGeneration);
        return;
    }

    LocalStorage::AutoCloudScanResult scan;
    try {
        scan = LocalStorage::GetAutoCloudFileList(GetSteamPath(), accountId, appId);
    } catch (const std::exception& ex) {
        // Any remaining exceptions are genuinely unexpected filesystem-API
        // failures, not the routine scan-limit condition (which now returns
        // structured state below). Clear the cache because we have no
        // confidence in internal state at this point.
        LOG("[AutoCloudImport] Scan failed for app %u: %s", appId, ex.what());
        ClearAutoCloudCanonicalTokens(accountId, appId, cacheGeneration);
        finish(false, cacheGeneration);
        return;
    } catch (...) {
        LOG("[AutoCloudImport] Scan failed for app %u", appId);
        ClearAutoCloudCanonicalTokens(accountId, appId, cacheGeneration);
        finish(false, cacheGeneration);
        return;
    }
    // Order matters: collision is OBSERVED evidence of misconfiguration —
    // it was actually seen in the scanned prefix. Scan-limit is a statement
    // about the UNOBSERVED tail. If both fire in the same scan, the
    // collision is already proof the rules are broken regardless of what
    // the unwalked tail contains, so treat it as corruption unconditionally.
    // Checking scanLimitHit first would let a genuine collision spin
    // forever on apps with large save trees: scan-limit preserves the
    // cache and forces a retry, and every retry that trips the cap again
    // would mask the collision indefinitely.
    if (scan.hasRootCollision) {
        // GetAutoCloudFileList already cleared scan.files. Clear the cache,
        // fail closed, and markAttempted=true so we don't loop on this app.
        LOG("[AutoCloudImport] Root collision detected for app %u; aborting bootstrap", appId);
        ClearAutoCloudCanonicalTokens(accountId, appId, cacheGeneration);
        finish(true, cacheGeneration);
        return;
    }
    if (scan.scanLimitHit) {
        // Scan-limit is a resource cap, not corruption. `scan.files` is a
        // truncated prefix — importing it would commit to canonicalizing
        // only part of the save set while the Steam client may reference
        // files beyond the cap. Fail closed on import (markAttempted=false
        // so a future session retries with possibly more generous conditions)
        // but preserve any canonical token cache that was already populated:
        // wiping it would strand incoming RPCs that rely on cached tokens
        // for files the scan DID observe successfully on a prior boot.
        LOG("[AutoCloudImport] Scan limit hit for app %u (%zu files observed); "
            "refusing partial import, preserving canonical token cache",
            appId, scan.files.size());
        finish(false, cacheGeneration);
        return;
    }
    std::vector<LocalStorage::FileEntry>& candidates = scan.files;
    if (candidates.empty()) {
        ClearAutoCloudCanonicalTokens(accountId, appId, cacheGeneration);
        finish(true, cacheGeneration);
        return;
    }
    size_t definitePollution = 0;
    for (const auto& fe : candidates) {
        if (LooksLikeForeignAppPollution(fe.filename, appId)) {
            LOG("[AutoCloudImport] Definite pollution candidate for app %u: %s", appId, fe.filename.c_str());
            ++definitePollution;
        }
    }
    if (definitePollution > 0) {
        LOG("[AutoCloudImport] Aborting import for app %u: %zu obvious pollution file(s) detected",
            appId, definitePollution);
        ClearAutoCloudCanonicalTokens(accountId, appId, cacheGeneration);
        finish(true, cacheGeneration);
        return;
    }

    std::unordered_map<std::string, LocalStorage::FileEntry> existing;
    for (const auto& fe : LocalStorage::GetFileList(accountId, appId)) {
        existing[fe.filename] = fe;
    }

    auto fileTokens = CloudStorage::LoadFileTokens(accountId, appId);
    auto rootTokens = CloudStorage::LoadRootTokens(accountId, appId);
    struct PendingImport {
        std::string filename;
        std::string sourcePath;
        uint64_t timestamp = 0;
        std::string rootToken;
    };
    std::vector<PendingImport> pendingImports;
    bool tokenMetadataChanged = false;

    for (const auto& fe : candidates) {
        // Per-file shutdown probe — this loop issues CheckBlobExists calls
        // which can each block on network I/O. Bail early without marking
        // the app attempted so the next session can retry.
        if (IsAutoCloudBootstrapShuttingDown()) {
            LOG("[AutoCloudImport] Aborting existence checks for app %u — shutdown in progress", appId);
            ClearAutoCloudCanonicalTokens(accountId, appId, cacheGeneration);
            finish(false, cacheGeneration);
            return;
        }
        if (fe.filename.empty() || fe.sourcePath.empty()) continue;
        if (IsInternalMetadataFile(fe.filename)) continue;

        auto it = existing.find(fe.filename);
        if (it != existing.end()) {
            if (!(it->second.sha == fe.sha && it->second.rawSize == fe.rawSize)) {
                LOG("[AutoCloudImport] Skipping existing app %u file %s to avoid overwriting cached/cloud data",
                    appId, fe.filename.c_str());
                continue;
            }

            auto existingToken = fileTokens.find(fe.filename);
            if (existingToken == fileTokens.end() || existingToken->second != fe.rootToken) {
                fileTokens[fe.filename] = fe.rootToken;
                tokenMetadataChanged = true;
                LOG("[AutoCloudImport] Canonical root token for app %u file %s: '%s'",
                    appId, fe.filename.c_str(), fe.rootToken.c_str());
            }
            if (!fe.rootToken.empty() && !rootTokens.count(fe.rootToken)) {
                rootTokens.insert(fe.rootToken);
                tokenMetadataChanged = true;
            }
            continue;
        }
        auto blobStatus = CloudStorage::CheckBlobExists(accountId, appId, fe.filename);
        if (blobStatus == ICloudProvider::ExistsStatus::Error) {
            LOG("[AutoCloudImport] Aborting import for app %u: could not verify cloud blob %s",
                appId, fe.filename.c_str());
            ClearAutoCloudCanonicalTokens(accountId, appId, cacheGeneration);
            finish(false, cacheGeneration);
            return;
        }
        if (blobStatus == ICloudProvider::ExistsStatus::Exists) {
            LOG("[AutoCloudImport] Skipping app %u file %s because blob already exists in cache/cloud",
                appId, fe.filename.c_str());
            continue;
        }
        if (HasPersistedLocalCloudHistory(accountId, appId)) {
            LOG("[AutoCloudImport] Skipping app %u file %s because existing CN may represent cloud deletion",
                appId, fe.filename.c_str());
            continue;
        }

        pendingImports.push_back({ fe.filename, fe.sourcePath, fe.timestamp, fe.rootToken });
    }

    if (pendingImports.empty() && !tokenMetadataChanged) {
        finish(true, cacheGeneration);
        return;
    }

    uint64_t publishGeneration = 0;
    size_t imported = 0;
    uint64_t cn = 0;
    // Hold g_autoCloudImportMutex only across the critical region that
    // reads cloud state, writes local blobs, and persists token metadata.
    // Blocking network calls (DrainQueueForApp, PushCNToCloudSync) run
    // OUTSIDE this mutex so unrelated RPC paths that call
    // InvalidateTokenCaches are not stalled on slow providers. The
    // generation check after the drains detects any concurrent
    // invalidation that may have raced in during the unlocked window.
    std::unique_lock<std::mutex> importLock(g_autoCloudImportMutex);
    if (!TryInvalidateTokenCachesForGeneration(accountId, appId, cacheGeneration, publishGeneration)) {
        finish(false, cacheGeneration);
        return;
    }
    // Last safe abort point — past this, StoreBlob writes local state and we
    // must see the CN commit through to keep disk consistent. If shutdown was
    // signaled during the scan/existence-check or while we waited on the
    // import lock, don't start writing blobs we'll never get to publish.
    if (IsAutoCloudBootstrapShuttingDown()) {
        LOG("[AutoCloudImport] Aborting pre-commit for app %u — shutdown in progress", appId);
        ClearAutoCloudCanonicalTokens(accountId, appId, publishGeneration);
        finish(false, publishGeneration);
        return;
    }
    {
        for (auto& pending : pendingImports) {
            if (HasPersistedLocalCloudHistory(accountId, appId)) {
                LOG("[AutoCloudImport] Skipping app %u file %s because CN appeared before commit",
                    appId, pending.filename.c_str());
                continue;
            }
            auto blobStatus = CloudStorage::CheckBlobExists(accountId, appId, pending.filename);
            if (blobStatus != ICloudProvider::ExistsStatus::Missing) {
                LOG("[AutoCloudImport] Skipping app %u file %s because blob appeared before commit",
                    appId, pending.filename.c_str());
                continue;
            }
            bool readOk = false;
            auto data = ReadWholeFile(pending.sourcePath, readOk);
            if (!readOk) {
                LOG("[AutoCloudImport] Failed to read source before commit for app %u: %s",
                    appId, pending.sourcePath.c_str());
                continue;
            }
            const uint8_t* ptr = data.empty() ? nullptr : data.data();
            if (!CloudStorage::StoreBlob(accountId, appId, pending.filename, ptr, data.size())) {
                LOG("[AutoCloudImport] Failed to cache app %u file %s", appId, pending.filename.c_str());
                continue;
            }
            auto existingToken = fileTokens.find(pending.filename);
            if (existingToken == fileTokens.end() || existingToken->second != pending.rootToken) {
                fileTokens[pending.filename] = pending.rootToken;
                tokenMetadataChanged = true;
                LOG("[AutoCloudImport] Canonical root token for app %u file %s: '%s'",
                    appId, pending.filename.c_str(), pending.rootToken.c_str());
            }
            if (!pending.rootToken.empty() && !rootTokens.count(pending.rootToken)) {
                rootTokens.insert(pending.rootToken);
                tokenMetadataChanged = true;
            }
            LocalStorage::SetFileTimestamp(accountId, appId, pending.filename, pending.timestamp);
            ++imported;
            LOG("[AutoCloudImport] Imported app %u file %s", appId, pending.filename.c_str());
        }
        if (imported == 0 && !tokenMetadataChanged) {
            finish(true, publishGeneration);
            return;
        }
        if (!rootTokens.empty()) CloudStorage::SaveRootTokens(accountId, appId, rootTokens);
        if (!fileTokens.empty() || tokenMetadataChanged) CloudStorage::SaveFileTokens(accountId, appId, fileTokens);
    }
    // Release the import mutex before any blocking network calls so unrelated
    // RPC paths that call InvalidateTokenCaches are not stalled on slow
    // providers. Generation checks before and after the network-bound work
    // detect concurrent invalidations that raced in during the unlocked
    // window.
    importLock.unlock();
    if (GetAutoCloudCanonicalTokenGeneration(accountId, appId) != publishGeneration) {
        finish(false, publishGeneration);
        return;
    }
    cn = LocalStorage::IncrementChangeNumber(accountId, appId);
    // CommitCNWithRetry drains + syncs CN; on failure it async-retries and
    // blocks on a second drain so process exit does not drop the publish.
    CloudStorage::CommitCNWithRetry(accountId, appId, cn);
    // Re-check generation before publishing canonical tokens; any concurrent
    // InvalidateTokenCaches during the unlocked drain window bumps the
    // generation and invalidates our cached candidates. The generation
    // compare-and-swap inside CacheAutoCloudCanonicalTokens is the actual
    // race guard; no additional publish mutex is needed.
    if (GetAutoCloudCanonicalTokenGeneration(accountId, appId) != publishGeneration) {
        finish(false, publishGeneration);
        return;
    }
    CacheAutoCloudCanonicalTokens(accountId, appId, candidates, publishGeneration);
    LOG("[AutoCloudImport] Imported %zu AutoCloud file(s), updatedTokens=%u for app %u, CN=%llu",
        imported, tokenMetadataChanged ? 1 : 0, appId, cn);
    finish(true, publishGeneration);
}

static void BootstrapAutoCloudFiles(uint32_t accountId, uint32_t appId, bool wait = false) {
    uint64_t cacheGeneration = GetAutoCloudCanonicalTokenGeneration(accountId, appId);
    if (!TryBeginAutoCloudBootstrap(accountId, appId)) {
        if (wait) WaitForAutoCloudBootstrap(accountId, appId);
        return;
    }
    if (wait) {
        BootstrapAutoCloudFilesWorker(accountId, appId, cacheGeneration);
        return;
    }
    std::lock_guard<std::mutex> lock(g_autoCloudBootstrapMutex);
    if (g_autoCloudBootstrapShuttingDown) {
        g_autoCloudBootstrapActiveApps.erase(MakeAppAccountKey(accountId, appId));
        g_autoCloudBootstrapCV.notify_all();
        return;
    }
    try {
        g_autoCloudBootstrapFutures.push_back(std::async(std::launch::async, [accountId, appId, cacheGeneration]() {
            BootstrapAutoCloudFilesWorker(accountId, appId, cacheGeneration);
        }));
    } catch (...) {
        g_autoCloudBootstrapActiveApps.erase(MakeAppAccountKey(accountId, appId));
        g_autoCloudBootstrapCV.notify_all();
    }
}

void ShutdownRpcHandlers() {
    std::vector<std::future<void>> futures;
    {
        std::lock_guard<std::mutex> lock(g_autoCloudBootstrapMutex);
        g_autoCloudBootstrapShuttingDown = true;
        futures.swap(g_autoCloudBootstrapFutures);
    }
    for (auto& future : futures) {
        try { future.get(); } catch (...) {}
    }
    std::unique_lock<std::mutex> lock(g_autoCloudBootstrapMutex);
    g_autoCloudBootstrapCV.wait(lock, [] {
        return g_autoCloudBootstrapActiveApps.empty();
    });
}

static uint64_t ParsePlaytimeField(const Json::Value& value) {
    if (value.type == Json::Type::Number) {
        return value.number() > 0 ? static_cast<uint64_t>(value.number()) : 0;
    }
    if (value.type == Json::Type::String) {
        return strtoull(value.str().c_str(), nullptr, 10);
    }
    return 0;
}

static void ParsePlaytimeBlob(const std::string& blob, uint64_t& lastPlayed,
                              uint64_t& playtime, uint64_t& playtime2wks) {
    auto parsed = Json::Parse(blob);
    if (parsed.type == Json::Type::Object) {
        if (parsed.has("LastPlayed"))
            lastPlayed = ParsePlaytimeField(parsed["LastPlayed"]);
        if (parsed.has("Playtime"))
            playtime = ParsePlaytimeField(parsed["Playtime"]);
        if (parsed.has("Playtime2wks"))
            playtime2wks = ParsePlaytimeField(parsed["Playtime2wks"]);
        if (playtime2wks == 0 && playtime > 0)
            playtime2wks = playtime;
        return;
    }

    std::istringstream blobStream(blob);
    std::string blobLine;
    while (std::getline(blobStream, blobLine)) {
        size_t tab = blobLine.find('\t');
        if (tab == std::string::npos) continue;
        std::string key = blobLine.substr(0, tab);
        std::string val = blobLine.substr(tab + 1);
        if (key == "LastPlayed") lastPlayed = strtoull(val.c_str(), nullptr, 10);
        else if (key == "Playtime") playtime = strtoull(val.c_str(), nullptr, 10);
        else if (key == "Playtime2wks") playtime2wks = strtoull(val.c_str(), nullptr, 10);
    }
    if (playtime2wks == 0 && playtime > 0)
        playtime2wks = playtime;
}

// per-app root tokens extracted from upload filenames (e.g., "%GameInstall%")
// populated when HandleBeginBatch or HandleBeginFileUpload sees a %Token% prefix.
// Used to know which tokens exist for an app; the changelist only presents each
// file under the specific token it was uploaded with (tracked in g_fileTokens).
static std::unordered_map<uint64_t, std::unordered_set<std::string>> g_appRootTokens;
static std::mutex g_rootTokenMutex;

// per-app file-to-token mapping: which root token each file was uploaded under.
// Key: (accountId, appId) -> { cleanName -> rootToken }
// This prevents the changelist from duplicating files across ALL tokens, which
// caused Steam's rootoverrides to see the cross-platform copy as stale and
// issue spurious deletes (killing the only actual blob).
static std::unordered_map<uint64_t, std::unordered_map<std::string, std::string>> g_fileTokens;
static std::mutex g_fileTokensMutex;

// Track which apps had file-token changes during the current batch.
// PersistFileTokens is deferred to HandleCompleteBatch instead of being
// called per-file, eliminating redundant file_tokens.dat cloud uploads.
static std::unordered_set<uint64_t> g_fileTokensDirtyApps;
static std::mutex g_fileTokensDirtyMutex;

// Per-batch AutoCloud canonical root map. Steam can upload stale local entries
// with their old root token even after our changelist advertises the canonical
// root. Resolve once for the batch and reuse for begin/commit handling.
static std::unordered_map<uint64_t, std::unordered_map<std::string, std::string>> g_batchCanonicalTokens;
static std::mutex g_batchCanonicalTokensMutex;


// Strip Steam root tokens like "%GameInstall%" from the start of a filename.
// Steam uses these as path prefixes (e.g. "%GameInstall%Saves/Slot1/file.dat")
// to indicate the file's root directory. Our local storage doesn't need them.
// Also strips any stray \r or \n between the token and the path.
static std::string StripRootToken(const std::string& filename) {
    if (filename.size() >= 2 && filename[0] == '%') {
        size_t end = filename.find('%', 1);
        if (end != std::string::npos && end + 1 < filename.size()) {
            size_t start = end + 1;
            // Skip any trailing \r or \n after the token (corrupted data cleanup)
            while (start < filename.size() && (filename[start] == '\r' || filename[start] == '\n'))
                ++start;
            return filename.substr(start);
        }
    }
    return filename;
}

// Extract just the root token (e.g., "%GameInstall%") from a filename.
// Returns empty string if no token present.
static std::string ExtractRootToken(const std::string& filename) {
    if (filename.size() >= 2 && filename[0] == '%') {
        size_t end = filename.find('%', 1);
        if (end != std::string::npos && end + 1 < filename.size()) {
            return filename.substr(0, end + 1); // e.g., "%GameInstall%"
        }
    }
    return "";
}

static void PrepareBatchCanonicalTokens(uint32_t accountId, uint32_t appId) {
    uint64_t key = MakeAppAccountKey(accountId, appId);
    {
        std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
        if (g_batchCanonicalTokens.find(key) != g_batchCanonicalTokens.end()) return;
    }

    std::unordered_map<std::string, std::string> tokens;
    {
        std::lock_guard<std::mutex> lock(g_autoCloudTokenCacheMutex);
        auto it = g_autoCloudCanonicalTokenCache.find(key);
        if (it != g_autoCloudCanonicalTokenCache.end()) {
            tokens = it->second;
        }
    }
    if (tokens.empty()) {
        tokens = CloudStorage::LoadFileTokens(accountId, appId);
    }
    if (tokens.empty()) {
        if (IsAutoCloudBootstrapActive(accountId, appId)) {
            WaitForAutoCloudBootstrap(accountId, appId);
            std::lock_guard<std::mutex> lock(g_autoCloudTokenCacheMutex);
            auto it = g_autoCloudCanonicalTokenCache.find(key);
            if (it != g_autoCloudCanonicalTokenCache.end()) tokens = it->second;
        }
        if (tokens.empty()) return;
    }

    std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
    g_batchCanonicalTokens.emplace(key, std::move(tokens));
}

static void ClearBatchCanonicalTokens(uint32_t accountId, uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
    g_batchCanonicalTokens.erase(MakeAppAccountKey(accountId, appId));
}

static std::string CanonicalizeUploadRootToken(uint32_t accountId, uint32_t appId,
                                               const std::string& cleanName,
                                               const std::string& fallbackToken) {
    if (cleanName.empty()) return fallbackToken;

    std::string canonical;
    bool foundCanonical = false;
    {
        std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
        auto appIt = g_batchCanonicalTokens.find(MakeAppAccountKey(accountId, appId));
        if (appIt != g_batchCanonicalTokens.end()) {
            auto tokenIt = appIt->second.find(cleanName);
            if (tokenIt != appIt->second.end()) {
                canonical = tokenIt->second;
                foundCanonical = true;
            }
        }
    }

    if (!foundCanonical) {
        std::lock_guard<std::mutex> lock(g_autoCloudTokenCacheMutex);
        auto appIt = g_autoCloudCanonicalTokenCache.find(MakeAppAccountKey(accountId, appId));
        if (appIt != g_autoCloudCanonicalTokenCache.end()) {
            auto tokenIt = appIt->second.find(cleanName);
            if (tokenIt != appIt->second.end()) {
                canonical = tokenIt->second;
                foundCanonical = true;
            }
        }
    }

    if (!foundCanonical) return fallbackToken;
    if (canonical != fallbackToken) {
        LOG("[NS-TOK] Canonicalized upload token for account %u app %u file %s: %s -> %s",
            accountId, appId, cleanName.c_str(), fallbackToken.c_str(), canonical.c_str());
    }
    return canonical;
}

// Capture a root token for an app from a filename containing a %Token% prefix.
// Tracked at two levels: g_appRootTokens (all tokens per app) and g_fileTokens
// (per-file -> token mapping for changelist). Returns true if new token added.
static bool TryCaptureRootToken(uint32_t accountId, uint32_t appId, const std::string& token) {
    if (token.empty()) return false;

    bool isNew = false;
    std::unordered_set<std::string> tokensCopy;
    uint64_t key = MakeAppAccountKey(accountId, appId);
    {
        std::lock_guard<std::mutex> lock(g_rootTokenMutex);
        auto& tokenSet = g_appRootTokens[key];
        auto result = tokenSet.insert(token);
        isNew = result.second;
        if (isNew) {
            LOG("[NS-TOK] Captured root token for account %u app %u: %s (now %zu tokens)",
                accountId, appId, token.c_str(), tokenSet.size());
            tokensCopy = tokenSet;  // copy under lock
        }
    }
    // Perform disk I/O + cloud upload outside the lock
    if (isNew) {
        CloudStorage::SaveRootTokens(accountId, appId, tokensCopy);
    }
    return isNew;
}

// Record which root token a file was uploaded under.
// Called from HandleCommitFileUpload after successful commit.
static void RecordFileToken(uint32_t accountId, uint32_t appId, const std::string& cleanName, const std::string& token) {
    if (cleanName.empty()) return;
    std::lock_guard<std::mutex> lock(g_fileTokensMutex);
    g_fileTokens[MakeAppAccountKey(accountId, appId)][cleanName] = token;
    LOG("[NS-FT] Recorded file token: account=%u app=%u file=%s token=%s",
        accountId, appId, cleanName.c_str(), token.c_str());
}

// Get the root token a file was uploaded under (empty if unknown).
static std::string GetFileToken(uint32_t accountId, uint32_t appId, const std::string& cleanName) {
    std::lock_guard<std::mutex> lock(g_fileTokensMutex);
    auto appIt = g_fileTokens.find(MakeAppAccountKey(accountId, appId));
    if (appIt == g_fileTokens.end()) return "";
    auto fileIt = appIt->second.find(cleanName);
    if (fileIt == appIt->second.end()) return "";
    return fileIt->second;
}

// Remove a file's token mapping (called on delete).
static void RemoveFileToken(uint32_t accountId, uint32_t appId, const std::string& cleanName) {
    std::lock_guard<std::mutex> lock(g_fileTokensMutex);
    auto appIt = g_fileTokens.find(MakeAppAccountKey(accountId, appId));
    if (appIt != g_fileTokens.end()) {
        appIt->second.erase(cleanName);
        LOG("[NS-FT] Removed file token: account=%u app=%u file=%s", accountId, appId, cleanName.c_str());
    }
}

// Save in-memory file token map to disk and cloud for a given app.
static void PersistFileTokens(uint32_t accountId, uint32_t appId) {
    std::unordered_map<std::string, std::string> snapshot;
    {
        std::lock_guard<std::mutex> lock(g_fileTokensMutex);
        auto it = g_fileTokens.find(MakeAppAccountKey(accountId, appId));
        if (it != g_fileTokens.end()) snapshot = it->second;
    }
    CloudStorage::SaveFileTokens(accountId, appId, snapshot);
}

// Mark an app's file tokens as needing persistence.
// Actual persist is deferred to HandleCompleteBatch to avoid
// redundant file_tokens.dat cloud uploads (one per file).
static void MarkFileTokensDirty(uint32_t accountId, uint32_t appId) {
    std::lock_guard<std::mutex> lock(g_fileTokensDirtyMutex);
    g_fileTokensDirtyApps.insert(MakeAppAccountKey(accountId, appId));
}

static void InvalidateTokenCaches(uint32_t accountId, uint32_t appId) {
    // Hold the import mutex so this cannot race with the import critical
    // region in BootstrapAutoCloudFilesWorker. Inner token-cache mutexes
    // are acquired in sequential scopes, matching the order documented at
    // the mutex-definitions block.
    std::lock_guard<std::mutex> importLock(g_autoCloudImportMutex);
    uint64_t key = MakeAppAccountKey(accountId, appId);
    {
        std::lock_guard<std::mutex> lock(g_rootTokenMutex);
        g_appRootTokens.erase(key);
    }
    {
        std::lock_guard<std::mutex> lock(g_fileTokensMutex);
        g_fileTokens.erase(key);
    }
    {
        std::lock_guard<std::mutex> lock(g_autoCloudTokenCacheMutex);
        g_autoCloudCanonicalTokenCache.erase(key);
        ++g_autoCloudCanonicalTokenGeneration[key];
    }
    {
        std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
        g_batchCanonicalTokens.erase(key);
    }
    {
        std::lock_guard<std::mutex> lock(g_autoCloudBootstrapMutex);
        g_autoCloudBootstrapAttemptedApps.erase(key);
    }
}

static bool TryInvalidateTokenCachesForGeneration(uint32_t accountId, uint32_t appId,
                                                  uint64_t generation,
                                                  uint64_t& nextGeneration) {
    uint64_t key = MakeAppAccountKey(accountId, appId);
    {
        std::lock_guard<std::mutex> lock(g_autoCloudTokenCacheMutex);
        if (g_autoCloudCanonicalTokenGeneration[key] != generation) return false;
        g_autoCloudCanonicalTokenCache.erase(key);
        nextGeneration = ++g_autoCloudCanonicalTokenGeneration[key];
    }
    {
        std::lock_guard<std::mutex> lock(g_rootTokenMutex);
        g_appRootTokens.erase(key);
    }
    {
        std::lock_guard<std::mutex> lock(g_fileTokensMutex);
        g_fileTokens.erase(key);
    }
    {
        std::lock_guard<std::mutex> lock(g_batchCanonicalTokensMutex);
        g_batchCanonicalTokens.erase(key);
    }
    {
        std::lock_guard<std::mutex> lock(g_autoCloudBootstrapMutex);
        g_autoCloudBootstrapAttemptedApps.erase(key);
    }
    return true;
}

static bool MergeStatsFile(uint32_t appId, uint32_t accountId,
                           const std::vector<uint8_t>& cloudData);

static bool FindVdfSectionRange(const std::string& vdfContent,
                                const char* const* sections,
                                size_t sectionCount,
                                size_t& sectionStart,
                                size_t& sectionEnd) {
    size_t searchPos = 0;
    for (size_t i = 0; i < sectionCount; ++i) {
        const std::string needle = std::string("\"") + sections[i] + "\"";
        size_t namePos = vdfContent.find(needle, searchPos);
        if (namePos == std::string::npos) return false;

        size_t openBrace = vdfContent.find('{', namePos + needle.size());
        if (openBrace == std::string::npos) return false;

        searchPos = openBrace + 1;
        if (i + 1 == sectionCount) {
            sectionStart = openBrace + 1;
            int depth = 1;
            for (size_t p = openBrace + 1; p < vdfContent.size(); ++p) {
                if (vdfContent[p] == '{') ++depth;
                else if (vdfContent[p] == '}') {
                    --depth;
                    if (depth == 0) {
                        sectionEnd = p;
                        return true;
                    }
                }
            }
            return false;
        }
    }
    return false;
}

static bool InsertPlaytimeFieldInSection(std::string& vdfContent,
                                         const char* const* sections,
                                         size_t sectionCount,
                                         std::string_view fieldName,
                                         const std::string& value) {
    size_t sectionStart = 0;
    size_t sectionEnd = 0;
    if (!FindVdfSectionRange(vdfContent, sections, sectionCount, sectionStart, sectionEnd)) {
        return false;
    }

    std::string indent = "\t";
    size_t lineStart = vdfContent.rfind('\n', sectionEnd);
    if (lineStart != std::string::npos) {
        ++lineStart;
        size_t indentEnd = lineStart;
        while (indentEnd < vdfContent.size() && (vdfContent[indentEnd] == '\t' || vdfContent[indentEnd] == ' ')) {
            ++indentEnd;
        }
        indent.assign(vdfContent.data() + lineStart, indentEnd - lineStart);
        indent.push_back('\t');
    }

    std::string insertion = indent + "\"" + std::string(fieldName) + "\"\t\t\"" + value + "\"\n";
    vdfContent.insert(sectionEnd, insertion);
    return true;
}

static bool EnsureVdfSectionPath(std::string& vdfContent,
                                 const char* const* sections,
                                 size_t sectionCount) {
    if (sectionCount == 0) return true;

    size_t sectionStart = 0;
    size_t sectionEnd = 0;
    if (FindVdfSectionRange(vdfContent, sections, sectionCount, sectionStart, sectionEnd)) {
        return true;
    }

    if (sectionCount == 1) {
        if (!vdfContent.empty() && vdfContent.back() != '\n') vdfContent.push_back('\n');
        vdfContent += "\"" + std::string(sections[0]) + "\"\n{\n}\n";
        return true;
    }

    if (!EnsureVdfSectionPath(vdfContent, sections, sectionCount - 1)) {
        return false;
    }

    size_t parentStart = 0;
    size_t parentEnd = 0;
    if (!FindVdfSectionRange(vdfContent, sections, sectionCount - 1, parentStart, parentEnd)) {
        return false;
    }

    std::string parentIndent = "\t";
    size_t lineStart = vdfContent.rfind('\n', parentEnd);
    if (lineStart != std::string::npos) {
        ++lineStart;
        size_t indentEnd = lineStart;
        while (indentEnd < vdfContent.size() && (vdfContent[indentEnd] == '\t' || vdfContent[indentEnd] == ' ')) {
            ++indentEnd;
        }
        parentIndent.assign(vdfContent.data() + lineStart, indentEnd - lineStart);
    }

    const std::string childIndent = parentIndent + "\t";
    std::string insertion;
    insertion += childIndent + "\"" + std::string(sections[sectionCount - 1]) + "\"\n";
    insertion += childIndent + "{\n";
    insertion += childIndent + "}\n";
    vdfContent.insert(parentEnd, insertion);
    return true;
}

static bool InsertPlaytimeAppSection(std::string& vdfContent,
                                     const char* const* sections,
                                     size_t sectionCount,
                                     const std::string& lastPlayed,
                                     const std::string& playtime,
                                     const std::string& playtime2wks) {
    if (!EnsureVdfSectionPath(vdfContent, sections, sectionCount)) {
        return false;
    }

    if (!InsertPlaytimeFieldInSection(vdfContent, sections, sectionCount, "LastPlayed", lastPlayed)) {
        return false;
    }
    if (!InsertPlaytimeFieldInSection(vdfContent, sections, sectionCount, "Playtime", playtime)) {
        return false;
    }
    if (!InsertPlaytimeFieldInSection(vdfContent, sections, sectionCount, "Playtime2wks", playtime2wks)) {
        return false;
    }
    return true;
}

static bool WriteLocalConfigWithRetry(const std::string& vdfPath, const std::string& vdfContent) {
    for (int attempt = 0; attempt < 10; ++attempt) {
        if (FileUtil::AtomicWriteText(vdfPath, vdfContent)) {
            return true;
        }
        Sleep(200);
    }
    return false;
}

static void RestorePlaytimeMetadata(uint32_t accountId, uint32_t appId, const std::vector<uint8_t>& ptData) {
    if (ptData.empty()) return;

    std::string blob(reinterpret_cast<const char*>(ptData.data()), ptData.size());
    uint64_t cloudLastPlayed = 0, cloudPlaytime = 0, cloudPlaytime2wks = 0;
    ParsePlaytimeBlob(blob, cloudLastPlayed, cloudPlaytime, cloudPlaytime2wks);

    if (cloudLastPlayed == 0 && cloudPlaytime == 0 && cloudPlaytime2wks == 0) {
        LOG("[Playtime] Cloud blob empty/invalid for app %u, skipping merge", appId);
        return;
    }

    std::string vdfPath = GetSteamPath() + "userdata\\" + std::to_string(accountId)
        + "\\config\\localconfig.vdf";
    std::ifstream vdfIn(vdfPath);
    if (!vdfIn.is_open()) {
        LOG("[Playtime] Cannot open localconfig.vdf for reading (app %u)", appId);
        return;
    }

    std::string vdfContent((std::istreambuf_iterator<char>(vdfIn)), {});
    vdfIn.close();

    std::string appIdStr = std::to_string(appId);
    const char* sections[] = { "UserLocalConfigStore", "Software", "Valve", "Steam", "Apps", appIdStr.c_str() };
    uint64_t localLastPlayed = 0, localPlaytime = 0, localPlaytime2wks = 0;

    struct FieldLoc { size_t valStart; size_t valEnd; };
    FieldLoc lpLoc = {0, 0}, ptLoc = {0, 0}, pt2Loc = {0, 0};

    bool found = VdfUtil::ForEachFieldInSection(vdfContent, sections, 6,
        [&](const VdfUtil::FieldInfo& fi) {
            if (fi.key == "LastPlayed") {
                localLastPlayed = strtoull(std::string(fi.value).c_str(), nullptr, 10);
                lpLoc = { fi.valStart, fi.valEnd };
            } else if (fi.key == "Playtime") {
                localPlaytime = strtoull(std::string(fi.value).c_str(), nullptr, 10);
                ptLoc = { fi.valStart, fi.valEnd };
            } else if (fi.key == "Playtime2wks") {
                localPlaytime2wks = strtoull(std::string(fi.value).c_str(), nullptr, 10);
                pt2Loc = { fi.valStart, fi.valEnd };
            }
            return true;
        });

    if (!found) {
        std::string newLP = std::to_string(cloudLastPlayed);
        std::string newPT = std::to_string(cloudPlaytime);
        std::string newPT2 = std::to_string(cloudPlaytime2wks);
        if (!InsertPlaytimeAppSection(vdfContent, sections, 6, newLP, newPT, newPT2)) {
            LOG("[Playtime] App %u section not found in localconfig.vdf, skipping merge", appId);
            return;
        }

        if (WriteLocalConfigWithRetry(vdfPath, vdfContent)) {
            RestoreInMemoryPlaytimeMetadata(appId, cloudLastPlayed, cloudPlaytime, cloudPlaytime2wks);
            LOG("[Playtime] Created playtime section for app %u: LastPlayed 0->%llu, Playtime 0->%llu, Playtime2wks 0->%llu",
                appId, cloudLastPlayed, cloudPlaytime, cloudPlaytime2wks);
        } else {
            LOG("[Playtime] Failed to write localconfig.vdf for app %u", appId);
        }
        return;
    }

    uint64_t mergedLP = (cloudLastPlayed > localLastPlayed) ? cloudLastPlayed : localLastPlayed;
    uint64_t mergedPT = (cloudPlaytime > localPlaytime) ? cloudPlaytime : localPlaytime;
    uint64_t mergedPT2 = (cloudPlaytime2wks > localPlaytime2wks) ? cloudPlaytime2wks : localPlaytime2wks;
    if (mergedLP == localLastPlayed && mergedPT == localPlaytime && mergedPT2 == localPlaytime2wks) {
        RestoreInMemoryPlaytimeMetadata(appId, mergedLP, mergedPT, mergedPT2);
        LOG("[Playtime] Local playtime already up-to-date for app %u", appId);
        return;
    }

    std::string newLP = std::to_string(mergedLP);
    std::string newPT = std::to_string(mergedPT);
    std::string newPT2 = std::to_string(mergedPT2);
    bool lpValid = lpLoc.valEnd > lpLoc.valStart;
    bool ptValid = ptLoc.valEnd > ptLoc.valStart;
    bool pt2Valid = pt2Loc.valEnd > pt2Loc.valStart;

    struct Replacement { size_t start; size_t len; std::string text; };
    std::vector<Replacement> reps;
    if (lpValid) reps.push_back({lpLoc.valStart, lpLoc.valEnd - lpLoc.valStart, newLP});
    if (ptValid) reps.push_back({ptLoc.valStart, ptLoc.valEnd - ptLoc.valStart, newPT});
    if (pt2Valid) reps.push_back({pt2Loc.valStart, pt2Loc.valEnd - pt2Loc.valStart, newPT2});

    if (!reps.empty()) {
        std::sort(reps.begin(), reps.end(),
            [](const Replacement& a, const Replacement& b) { return a.start > b.start; });
        for (auto& r : reps)
            vdfContent.replace(r.start, r.len, r.text);
    }

    bool inserted = false;
    if (!lpValid) {
        inserted = InsertPlaytimeFieldInSection(vdfContent, sections, 6, "LastPlayed", newLP) || inserted;
    }
    if (!ptValid) {
        inserted = InsertPlaytimeFieldInSection(vdfContent, sections, 6, "Playtime", newPT) || inserted;
    }
    if (!pt2Valid) {
        inserted = InsertPlaytimeFieldInSection(vdfContent, sections, 6, "Playtime2wks", newPT2) || inserted;
    }
    if (!lpValid && !ptValid && !pt2Valid && !inserted) {
        LOG("[Playtime] App %u section has no playtime fields, skipping write", appId);
        return;
    }

    if (WriteLocalConfigWithRetry(vdfPath, vdfContent)) {
        RestoreInMemoryPlaytimeMetadata(appId, mergedLP, mergedPT, mergedPT2);
        LOG("[Playtime] Merged playtime for app %u: LastPlayed %llu->%llu, Playtime %llu->%llu, Playtime2wks %llu->%llu",
            appId, localLastPlayed, mergedLP, localPlaytime, mergedPT, localPlaytime2wks, mergedPT2);
    } else {
        LOG("[Playtime] Failed to write localconfig.vdf for app %u", appId);
    }
}

void RestoreAppMetadata(uint32_t accountId, uint32_t appId) {
    InvalidateTokenCaches(accountId, appId);

    auto statsData = CloudStorage::RetrieveBlob(accountId, appId, kStatsMetadataPath);
    if (!statsData.empty()) {
        MergeStatsFile(appId, accountId, statsData);
    }

    auto ptData = CloudStorage::RetrieveBlob(accountId, appId, kPlaytimeMetadataPath);
    RestorePlaytimeMetadata(accountId, appId, ptData);
}


static std::string GetMachineName() {
    char buf[MAX_COMPUTERNAME_LENGTH + 1];
    DWORD len = sizeof(buf);
    if (GetComputerNameA(buf, &len))
        return std::string(buf, len);
    return "UNKNOWN";
}


uint32_t ExtractAppId(const char* method, const std::vector<PB::Field>& body) {
    uint32_t fieldNum = 1;
    if (strcmp(method, RPC_COMMIT_UPLOAD) == 0) fieldNum = 2;
    auto* f = PB::FindField(body, fieldNum);
    return f ? (uint32_t)f->varintVal : 0;
}


// Returns file list from HttpServer blob store (what's been uploaded).
// Steam compares this against remotecache.vdf to decide uploads/downloads.
PB::Writer HandleGetChangelist(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    auto* cnField = PB::FindField(reqBody, 2);
    uint64_t clientChangeNumber = cnField ? cnField->varintVal : 0;

    uint32_t accountId = 0;
    if (!RequireAccountId("GetAppFileChangelist", appId, accountId)) {
        PB::Writer body;
        body.WriteVarint(1, 0);
        body.WriteVarint(3, 0);
        body.WriteString(5, GetMachineName());
        body.WriteVarint(6, 0);
        return body;
    }
    uint64_t appKey = MakeAppAccountKey(accountId, appId);

    BootstrapAutoCloudFiles(accountId, appId, /*wait=*/true);

    // Filenames from GetFileList are generated by filesystem::relative() against a controlled
    // app root directory, so they cannot contain path traversal sequences (e.g. "../").
    auto files = LocalStorage::GetFileList(accountId, appId);
    uint64_t serverChangeNumber = LocalStorage::GetChangeNumber(accountId, appId);

    files.erase(std::remove_if(files.begin(), files.end(),
        [](const LocalStorage::FileEntry& fe) {
            return IsInternalMetadataFile(fe.filename);
        }), files.end());

    LOG("[NS-CL] GetAppFileChangelist app=%u clientCN=%llu serverCN=%llu files=%zu",
        appId, clientChangeNumber, serverChangeNumber, files.size());

    // build path_prefix table and file entries
    std::unordered_map<std::string, uint32_t> prefixMap;
    std::vector<std::string> prefixList;
    std::string machineName = GetMachineName();

    // Look up ALL root tokens for this app (captured from upload filenames).
    std::unordered_set<std::string> rootTokens;
    {
        std::lock_guard<std::mutex> lock(g_rootTokenMutex);
        auto it = g_appRootTokens.find(appKey);
        if (it != g_appRootTokens.end()) {
            rootTokens = it->second;
        }
    }
    // If not in memory, try loading from disk (persisted from previous session)
    if (rootTokens.empty()) {
        rootTokens = CloudStorage::LoadRootTokens(accountId, appId);
        if (!rootTokens.empty()) {
            std::lock_guard<std::mutex> lock(g_rootTokenMutex);
            g_appRootTokens[appKey] = rootTokens;
        }
    }
    // If still empty, use empty string (no root token prefix -- legacy behavior)
    if (rootTokens.empty()) {
        rootTokens.insert("");
    }

    for (auto& t : rootTokens) {
        LOG("[NS-CL] Root token for app %u: '%s'", appId, t.c_str());
    }

    // Load per-file token map (which token each file was uploaded under).
    // If not in memory, load from disk. Snapshot the map so we can do
    // lockless lookups in the file loop below (H5: was N lock acquisitions).
    std::unordered_map<std::string, std::string> fileTokenSnapshot;
    {
        std::lock_guard<std::mutex> lock(g_fileTokensMutex);
        if (g_fileTokens.find(appKey) == g_fileTokens.end()) {
            auto loaded = CloudStorage::LoadFileTokens(accountId, appId);
            if (!loaded.empty()) {
                g_fileTokens[appKey] = std::move(loaded);
                LOG("[NS-CL] Loaded %zu file-token mappings for account %u app %u",
                    g_fileTokens[appKey].size(), accountId, appId);
            }
        }
        auto it = g_fileTokens.find(appKey);
        if (it != g_fileTokens.end()) {
            fileTokenSnapshot = it->second;
        }
    }

    // Pick a default token for files with no recorded token
    // (e.g., files synced from cloud before file_tokens.dat existed).
    // Prefer %GameInstall% if available, otherwise pick the lexicographically
    // smallest token for deterministic behavior across restarts.
    std::string defaultToken;
    if (!rootTokens.empty()) {
        if (rootTokens.count("%GameInstall%"))
            defaultToken = "%GameInstall%";
        else {
            std::vector<std::string> sorted(rootTokens.begin(), rootTokens.end());
            std::sort(sorted.begin(), sorted.end());
            defaultToken = sorted.front();
        }
    }

    struct PreparedFile {
        std::string leaf;
        uint32_t prefixIdx;
        const LocalStorage::FileEntry* entry;
    };
    std::vector<PreparedFile> prepared;

    for (auto& fe : files) {
        // split filename into directory prefix + leaf
        size_t lastSlash = fe.filename.rfind('/');
        std::string dirPrefix, leaf;
        if (lastSlash != std::string::npos) {
            dirPrefix = fe.filename.substr(0, lastSlash + 1);
            leaf = fe.filename.substr(lastSlash + 1);
        } else {
            leaf = fe.filename;
        }

        // Look up which token this specific file was uploaded under.
        // Only emit it under THAT token -- not all tokens.
        // This prevents Steam's rootoverrides from seeing cross-platform
        // duplicates and issuing spurious deletes.
        // Uses pre-loop snapshot (H5: avoids per-file mutex acquisition).
        std::string fileToken;
        auto ftIt = fileTokenSnapshot.find(fe.filename);
        bool hasRecordedFileToken = ftIt != fileTokenSnapshot.end();
        if (hasRecordedFileToken) fileToken = ftIt->second;
        if (!hasRecordedFileToken) {
            fileToken = defaultToken;
            LOG("[NS-CL]   file: %s has no recorded token, using default '%s'",
                fe.filename.c_str(), fileToken.c_str());
        }

        std::string fullPrefix = fileToken + dirPrefix;

        uint32_t prefixIdx;
        auto it = prefixMap.find(fullPrefix);
        if (it != prefixMap.end()) {
            prefixIdx = it->second;
        } else {
            prefixIdx = (uint32_t)prefixList.size();
            prefixMap[fullPrefix] = prefixIdx;
            prefixList.push_back(fullPrefix);
        }

        prepared.push_back({leaf, prefixIdx, &fe});
        LOG("[NS-CL]   file: %s (prefix[%u]=%s, size=%llu, ts=%llu)",
            fe.filename.c_str(), prefixIdx, fullPrefix.c_str(), fe.rawSize, fe.timestamp);
    }

    PB::Writer body;
    body.WriteVarint(1, serverChangeNumber);                     // current_change_number
    // is_only_delta: false = full listing (simplest, always correct)
    body.WriteVarint(3, 0);

    // file entries (field 2, repeated)
    for (auto& pf : prepared) {
        PB::Writer fileSub;
        fileSub.WriteString(1, pf.leaf);                        // file_name (leaf only)
        if (!pf.entry->sha.empty())
            fileSub.WriteBytes(2, pf.entry->sha.data(), pf.entry->sha.size()); // sha_file
        fileSub.WriteVarint(3, pf.entry->timestamp);            // time_stamp
        fileSub.WriteVarint(4, pf.entry->rawSize);              // raw_file_size
        fileSub.WriteVarint(5, 0);                              // persist_state = persisted
        fileSub.WriteVarint(6, 0xFFFFFFFF);                     // platforms_to_sync = all
        fileSub.WriteVarint(7, pf.prefixIdx);                    // path_prefix_index (0 = first real prefix)
        fileSub.WriteVarint(8, 0);                              // machine_name_index
        body.WriteSubmessage(2, fileSub);
    }

    // path_prefixes (field 4, repeated)
    for (auto& p : prefixList) {
        body.WriteString(4, p);
    }

    // machine_names (field 5, repeated)
    body.WriteString(5, machineName);

    // app_buildid_hwm (field 6) -- not critical, set to 0
    body.WriteVarint(6, 0);

    LOG("[NS-CL] Response: %zu files, %zu prefixes, CN=%llu",
        prepared.size(), prefixList.size(), serverChangeNumber);

    // Hex dump our generated response for comparison with real Steam
#ifdef DEBUG_HEX_DUMP
    {
        auto& ourData = body.Data();
        LOG("[NS-CL-HEX] Our changelist response: %zu bytes", ourData.size());
        for (size_t off = 0; off < ourData.size(); off += 32) {
            char hexLine[200];
            int pos = 0;
            size_t end = (off + 32 < ourData.size()) ? off + 32 : ourData.size();
            for (size_t i = off; i < end; i++) {
                pos += snprintf(hexLine + pos, sizeof(hexLine) - pos, "%02X ", ourData[i]);
            }
            LOG("[NS-CL-HEX] offset=%04X: %s", (unsigned)off, hexLine);
        }
    }
#endif

    return body;
}

// --- Binary KV reader/writer for UserGameStats merge ---

enum BkvType : uint8_t {
    BKV_SECTION   = 0x00,
    BKV_STRING    = 0x01,
    BKV_INT       = 0x02,
    BKV_FLOAT     = 0x03,
    BKV_UINT64    = 0x07,
    BKV_END       = 0x08,
    BKV_INT64     = 0x0A,
};

struct BkvNode {
    BkvType type;
    std::string name;
    // value storage (union-like, depends on type)
    uint32_t intVal = 0;
    float floatVal = 0.0f;
    uint64_t uint64Val = 0;
    int64_t int64Val = 0;
    std::string strVal;
    std::vector<BkvNode> children; // for BKV_SECTION
};

static constexpr int BKV_MAX_DEPTH = 128;
static constexpr size_t BKV_MAX_NODES = 100000;

static bool BkvRead(const uint8_t* data, size_t len, size_t& pos, std::vector<BkvNode>& out, int depth, size_t& totalNodes) {
    if (depth > BKV_MAX_DEPTH) {
        LOG("[Stats] BKV nesting too deep (%d), aborting parse", depth);
        return false;
    }
    while (pos < len) {
        uint8_t tag = data[pos++];
        if (tag == BKV_END)
            return true;

        BkvNode node;
        node.type = static_cast<BkvType>(tag);

        // read null-terminated name
        const char* nameStart = reinterpret_cast<const char*>(data + pos);
        size_t nameEnd = pos;
        while (nameEnd < len && data[nameEnd] != 0) nameEnd++;
        if (nameEnd >= len) return false;
        node.name.assign(nameStart, nameEnd - pos);
        pos = nameEnd + 1;

        switch (node.type) {
        case BKV_SECTION:
            if (!BkvRead(data, len, pos, node.children, depth + 1, totalNodes))
                return false;
            break;
        case BKV_STRING: {
            const char* s = reinterpret_cast<const char*>(data + pos);
            size_t end = pos;
            while (end < len && data[end] != 0) end++;
            if (end >= len) return false;
            node.strVal.assign(s, end - pos);
            pos = end + 1;
            break;
        }
        case BKV_INT:
        case BKV_FLOAT:
            if (pos + 4 > len) return false;
            if (node.type == BKV_INT)
                memcpy(&node.intVal, data + pos, 4);
            else
                memcpy(&node.floatVal, data + pos, 4);
            pos += 4;
            break;
        case BKV_UINT64:
            if (pos + 8 > len) return false;
            memcpy(&node.uint64Val, data + pos, 8);
            pos += 8;
            break;
        case BKV_INT64:
            if (pos + 8 > len) return false;
            memcpy(&node.int64Val, data + pos, 8);
            pos += 8;
            break;
        default:
            LOG("[Stats] Unknown BKV tag 0x%02X at offset %zu", tag, pos - 1);
            return false;
        }
        if (++totalNodes > BKV_MAX_NODES) {
            LOG("[Stats] BKV node limit exceeded (%zu), aborting parse", totalNodes);
            return false;
        }
        out.push_back(std::move(node));
    }
    return depth == 0;
}

static void BkvWrite(const std::vector<BkvNode>& nodes, std::vector<uint8_t>& out) {
    for (auto& n : nodes) {
        out.push_back(static_cast<uint8_t>(n.type));
        out.insert(out.end(), n.name.begin(), n.name.end());
        out.push_back(0);

        switch (n.type) {
        case BKV_SECTION:
            BkvWrite(n.children, out);
            out.push_back(BKV_END);
            break;
        case BKV_STRING:
            out.insert(out.end(), n.strVal.begin(), n.strVal.end());
            out.push_back(0);
            break;
        case BKV_INT:
            out.insert(out.end(), reinterpret_cast<const uint8_t*>(&n.intVal),
                       reinterpret_cast<const uint8_t*>(&n.intVal) + 4);
            break;
        case BKV_FLOAT:
            out.insert(out.end(), reinterpret_cast<const uint8_t*>(&n.floatVal),
                       reinterpret_cast<const uint8_t*>(&n.floatVal) + 4);
            break;
        case BKV_UINT64:
            out.insert(out.end(), reinterpret_cast<const uint8_t*>(&n.uint64Val),
                       reinterpret_cast<const uint8_t*>(&n.uint64Val) + 8);
            break;
        case BKV_INT64:
            out.insert(out.end(), reinterpret_cast<const uint8_t*>(&n.int64Val),
                       reinterpret_cast<const uint8_t*>(&n.int64Val) + 8);
            break;
        default:
            break;
        }
    }
}

static BkvNode* BkvFind(std::vector<BkvNode>& nodes, const std::string& name) {
    for (auto& n : nodes)
        if (n.name == name) return &n;
    return nullptr;
}

// Merge cloud stats into local stats (monotonic: more achievements/stats wins).
// Returns merged node tree ready to write.
static std::vector<BkvNode> MergeStats(
    std::vector<BkvNode>& local, std::vector<BkvNode>& cloud)
{
    // Top level should be a single "cache" section in each
    BkvNode* localCache = BkvFind(local, "cache");
    BkvNode* cloudCache = BkvFind(cloud, "cache");
    if (!localCache || !cloudCache) {
        // If either is missing/malformed, prefer whichever has a cache section
        if (cloudCache) return std::move(cloud);
        return std::move(local);
    }

    // Walk cloud stat sections and merge into local
    for (auto& cloudStat : cloudCache->children) {
        if (cloudStat.type != BKV_SECTION) continue;
        // skip non-stat sections (crc, PendingChanges are INT not SECTION)

        BkvNode* localStat = BkvFind(localCache->children, cloudStat.name);
        if (!localStat) {
            // stat exists in cloud but not locally -- take it
            localCache->children.push_back(cloudStat);
            continue;
        }

        BkvNode* localData = BkvFind(localStat->children, "data");
        BkvNode* cloudData = BkvFind(cloudStat.children, "data");
        if (!localData || !cloudData) continue;

        BkvNode* cloudAchTimes = BkvFind(cloudStat.children, "AchievementTimes");
        BkvNode* localAchTimes = BkvFind(localStat->children, "AchievementTimes");

        if (cloudAchTimes || localAchTimes) {
            // Achievement stat: OR the bitfields
            localData->intVal |= cloudData->intVal;

            // Ensure local has an AchievementTimes section
            if (!localAchTimes) {
                localStat->children.push_back(BkvNode{BKV_SECTION, "AchievementTimes"});
                localAchTimes = &localStat->children.back();
            }

            // Merge timestamps: for each bit index, keep earliest nonzero
            if (cloudAchTimes) {
                for (auto& ct : cloudAchTimes->children) {
                    if (ct.type != BKV_INT) continue;
                    BkvNode* lt = BkvFind(localAchTimes->children, ct.name);
                    if (!lt) {
                        localAchTimes->children.push_back(ct);
                    } else if (ct.intVal != 0 && (lt->intVal == 0 || ct.intVal < lt->intVal)) {
                        lt->intVal = ct.intVal;
                    }
                }
            }
        } else {
            // Regular stat: take max
            if (localData->type == BKV_INT && cloudData->type == BKV_INT) {
                if (cloudData->intVal > localData->intVal)
                    localData->intVal = cloudData->intVal;
            } else if (localData->type == BKV_FLOAT && cloudData->type == BKV_FLOAT) {
                if (cloudData->floatVal > localData->floatVal)
                    localData->floatVal = cloudData->floatVal;
            } else if (localData->type == BKV_UINT64 && cloudData->type == BKV_UINT64) {
                if (cloudData->uint64Val > localData->uint64Val)
                    localData->uint64Val = cloudData->uint64Val;
            } else if (localData->type == BKV_INT64 && cloudData->type == BKV_INT64) {
                if (cloudData->int64Val > localData->int64Val)
                    localData->int64Val = cloudData->int64Val;
            }
        }
    }

    // Recalculate CRC: set to 0 so Steam recalculates on load
    BkvNode* crc = BkvFind(localCache->children, "crc");
    if (crc && crc->type == BKV_INT)
        crc->intVal = 0;

    return std::move(local);
}

// Merge cloud stats into the local stats file on disk.
static bool MergeStatsFile(uint32_t appId, uint32_t accountId,
                           const std::vector<uint8_t>& cloudData)
{
    std::string statsPath = GetSteamPath() + "appcache\\stats\\UserGameStats_"
        + std::to_string(accountId) + "_" + std::to_string(appId) + ".bin";

    // Parse cloud data
    size_t cloudPos = 0;
    size_t cloudNodeCount = 0;
    std::vector<BkvNode> cloudNodes;
    if (!BkvRead(cloudData.data(), cloudData.size(), cloudPos, cloudNodes, 0, cloudNodeCount)) {
        LOG("[Stats] Failed to parse cloud stats for app %u, skipping merge", appId);
        return false;
    }

    // Read local file
    std::ifstream localFile(statsPath, std::ios::binary | std::ios::ate);
    if (!localFile.is_open()) {
        // No local file -- parse and rewrite cloud data to strip junk
        std::vector<uint8_t> outBuf;
        BkvWrite(cloudNodes, outBuf);
        if (!FileUtil::AtomicWriteBinary(statsPath, outBuf.data(), outBuf.size())) {
            LOG("[Stats] Failed to create stats file for app %u", appId);
            return false;
        }
        LOG("[Stats] No local stats, wrote cloud stats for app %u (%zu bytes)", appId, outBuf.size());
        return true;
    }

    auto localSize = localFile.tellg();
    if (localSize <= 0) {
        localFile.close();
        std::vector<uint8_t> outBuf;
        BkvWrite(cloudNodes, outBuf);
        if (!FileUtil::AtomicWriteBinary(statsPath, outBuf.data(), outBuf.size()))
            return false;
        LOG("[Stats] Local stats empty, wrote cloud stats for app %u (%zu bytes)", appId, outBuf.size());
        return true;
    }

    std::vector<uint8_t> localData(static_cast<size_t>(localSize));
    localFile.seekg(0);
    localFile.read(reinterpret_cast<char*>(localData.data()), localSize);
    localFile.close();

    // Parse local data
    size_t localPos = 0;
    size_t localNodeCount = 0;
    std::vector<BkvNode> localNodes;
    if (!BkvRead(localData.data(), localData.size(), localPos, localNodes, 0, localNodeCount)) {
        LOG("[Stats] Failed to parse local stats for app %u, overwriting with cloud", appId);
        std::ofstream f(statsPath, std::ios::binary | std::ios::trunc);
        if (!f.is_open()) return false;
        f.write(reinterpret_cast<const char*>(cloudData.data()), cloudData.size());
        return true;
    }

    // Merge
    auto merged = MergeStats(localNodes, cloudNodes);

    // Serialize
    std::vector<uint8_t> outBuf;
    BkvWrite(merged, outBuf);

    // Write atomically (tmp + rename) to avoid partial reads on crash
    if (!FileUtil::AtomicWriteBinary(statsPath, outBuf.data(), outBuf.size())) {
        LOG("[Stats] Failed to write merged stats for app %u", appId);
        return false;
    }

    LOG("[Stats] Merged stats for app %u (local=%zu cloud=%zu merged=%zu bytes)",
        appId, localData.size(), cloudData.size(), outBuf.size());
    return true;
}

// SignalAppLaunchIntent
// Steam calls this before sync. We respond with empty pending_remote_operations.
PB::Writer HandleLaunchIntent(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    LOG("[NS] SignalAppLaunchIntent app=%u", appId);

    RecordLaunchTime(appId);
    // Pull latest data from cloud provider (if active) before game starts.
    // This downloads CN, root tokens, metadata, and any missing blobs.
    uint32_t accountId = 0;
    if (!RequireAccountId("SignalAppLaunchIntent", appId, accountId)) {
        PB::Writer body;
        return body;
    }
    if (CloudStorage::IsCloudActive()) {
        LOG("[NS] Syncing app %u from cloud (%s) before launch...",
            appId, CloudStorage::ProviderName());
        bool hadNewer = CloudStorage::SyncFromCloud(accountId, appId);
        LOG("[NS] Cloud sync complete for app %u (hadNewer=%d)", appId, hadNewer);
        RestoreAppMetadata(accountId, appId);
    }
    BootstrapAutoCloudFiles(accountId, appId, /*wait=*/true);

    PB::Writer body; // empty = no pending remote operations
    return body;
}

// ClientGetAppQuotaUsage
PB::Writer HandleQuotaUsage(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    uint32_t accountId = 0;
    if (!RequireAccountId("ClientGetAppQuotaUsage", appId, accountId)) {
        PB::Writer body;
        body.WriteVarint(1, 0);
        body.WriteVarint(2, 0);
        body.WriteVarint(3, 10000);
        body.WriteVarint(4, 1073741824ULL);
        return body;
    }

    BootstrapAutoCloudFiles(accountId, appId);

    auto files = LocalStorage::GetFileList(accountId, appId);
    files.erase(std::remove_if(files.begin(), files.end(),
        [](const LocalStorage::FileEntry& fe) {
            return IsInternalMetadataFile(fe.filename);
        }), files.end());
    uint64_t totalBytes = 0;
    for (auto& f : files) totalBytes += f.rawSize;

    PB::Writer body;
    body.WriteVarint(1, (uint64_t)files.size());    // existing_files
    body.WriteVarint(2, totalBytes);                 // existing_bytes
    body.WriteVarint(3, 10000);                      // max_num_files
    body.WriteVarint(4, 1073741824ULL);              // max_num_bytes (1 GB)

    LOG("[NS] QuotaUsage app=%u files=%zu bytes=%llu", appId, files.size(), totalBytes);
    return body;
}

// BeginAppUploadBatch
PB::Writer HandleBeginBatch(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    uint64_t batchId = g_nextBatchId.fetch_add(1);
    uint32_t accountId = 0;
    if (!RequireAccountId("BeginAppUploadBatch", appId, accountId)) {
        PB::Writer body;
        body.WriteVarint(1, batchId);
        body.WriteVarint(4, 0);
        return body;
    }
    uint64_t changeNumber = LocalStorage::GetChangeNumber(accountId, appId);
    PrepareBatchCanonicalTokens(accountId, appId);

    // log files to upload/delete and capture root tokens from filenames
    int uploadCount = 0, deleteCount = 0;
    for (auto& f : reqBody) {
        if (f.fieldNum == 3 && f.wireType == PB::LengthDelimited) {
            std::string name(reinterpret_cast<const char*>(f.data), f.dataLen);
            LOG("[NS-BATCH]   upload: %s", name.c_str());
            TryCaptureRootToken(accountId, appId,
                CanonicalizeUploadRootToken(accountId, appId, StripRootToken(name), ExtractRootToken(name)));
            ++uploadCount;
        }
        if (f.fieldNum == 4 && f.wireType == PB::LengthDelimited) {
            std::string name(reinterpret_cast<const char*>(f.data), f.dataLen);
            LOG("[NS-BATCH]   delete: %s", name.c_str());
            TryCaptureRootToken(accountId, appId,
                CanonicalizeUploadRootToken(accountId, appId, StripRootToken(name), ExtractRootToken(name)));
            ++deleteCount;
        }
    }

    PB::Writer body;
    body.WriteVarint(1, batchId);                    // batch_id
    body.WriteVarint(4, changeNumber);               // app_change_number

    LOG("[NS] BeginBatch app=%u batchId=%llu uploads=%d deletes=%d",
        appId, batchId, uploadCount, deleteCount);
    return body;
}

// ClientBeginFileUpload
// Tell Steam to PUT the file to our local HTTP server.
PB::Writer HandleBeginFileUpload(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    // extract request fields
    uint64_t fileSize = 0, rawFileSize = 0;
    std::string filename;
    std::vector<uint8_t> fileSha;
    uint64_t timestamp = 0;

    for (auto& f : reqBody) {
        if (f.fieldNum == 2 && f.wireType == PB::Varint) fileSize = f.varintVal;
        if (f.fieldNum == 3 && f.wireType == PB::Varint) rawFileSize = f.varintVal;
        if (f.fieldNum == 4 && f.wireType == PB::LengthDelimited)
            fileSha.assign(f.data, f.data + f.dataLen);
        if (f.fieldNum == 5 && f.wireType == PB::Varint) timestamp = f.varintVal;
        if (f.fieldNum == 6 && f.wireType == PB::LengthDelimited)
            filename.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
    }

    uint16_t port = HttpServer::GetPort();
    uint32_t accountId = 0;
    if (!RequireAccountId("ClientBeginFileUpload", appId, accountId)) {
        return PB::Writer();
    }
    std::string urlHost = "127.0.0.1:" + std::to_string(port);
    std::string rootToken = ExtractRootToken(filename);
    std::string cleanName = StripRootToken(filename);
    PrepareBatchCanonicalTokens(accountId, appId);
    rootToken = CanonicalizeUploadRootToken(accountId, appId, cleanName, rootToken);
    std::string urlPath = "/upload/" + std::to_string(accountId) + "/" + std::to_string(appId)
        + "/" + HttpUtil::UrlEncode(cleanName, true);

    // Remember the root token for this app (used for default fallback in changelist).
    // The per-file token mapping (g_fileTokens) is set in HandleCommitFileUpload
    // after the upload succeeds.
    TryCaptureRootToken(accountId, appId, rootToken);

    LOG("[NS-UP] BeginFileUpload app=%u file=%s (clean=%s) size=%llu rawSize=%llu -> %s%s",
        appId, filename.c_str(), cleanName.c_str(), fileSize, rawFileSize, urlHost.c_str(), urlPath.c_str());

    // Steam has the data at fileSize bytes (possibly ZIP-compressed).
    // Request exactly that many bytes so the PUT actually happens.
    uint64_t blockLen = fileSize > 0 ? fileSize : rawFileSize;

    // build block request submessage (ClientCloudFileUploadBlockDetails)
    PB::Writer blockReq;
    blockReq.WriteString(1, urlHost);                // url_host
    blockReq.WriteString(2, urlPath);                // url_path
    blockReq.WriteVarint(3, 0);                      // use_https = false
    blockReq.WriteVarint(4, 4);                      // http_method = PUT (EHTTPMethod: 4)
    // no request_headers needed for our simple server
    blockReq.WriteVarint(6, 0);                      // block_offset = 0
    blockReq.WriteVarint(7, blockLen);               // block_length

    PB::Writer body;
    body.WriteVarint(1, 0);                          // encrypt_file = false
    body.WriteSubmessage(2, blockReq);               // block_requests (repeated, just 1)

    // hex dump response for debugging upload failures
#ifdef DEBUG_HEX_DUMP
    {
        auto& d = body.Data();
        std::string hex;
        for (size_t i = 0; i < d.size(); i++) {
            char tmp[4]; snprintf(tmp, sizeof(tmp), "%02X ", d[i]);
            hex += tmp;
        }
        LOG("[NS-UP] Response hex (%zu bytes): %s", d.size(), hex.c_str());
        auto& bd = blockReq.Data();
        std::string bhex;
        for (size_t i = 0; i < bd.size(); i++) {
            char tmp[4]; snprintf(tmp, sizeof(tmp), "%02X ", bd[i]);
            bhex += tmp;
        }
        LOG("[NS-UP] BlockReq hex (%zu bytes): %s", bd.size(), bhex.c_str());
    }
#endif

    return body;
}

// ClientCommitFileUpload
// The file has been PUT to our HTTP server. Update local metadata.
PB::Writer HandleCommitFileUpload(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    bool transferSucceeded = false;
    std::string filename;

    for (auto& f : reqBody) {
        if (f.fieldNum == 1 && f.wireType == PB::Varint) transferSucceeded = (f.varintVal != 0);
        if (f.fieldNum == 4 && f.wireType == PB::LengthDelimited)
            filename.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
    }

    LOG("[NS-UP] CommitFileUpload app=%u file=%s succeeded=%d",
        appId, filename.c_str(), transferSucceeded);

    std::string cleanName = StripRootToken(filename);
    bool committed = false;
    uint32_t accountId = 0;
    if (!RequireAccountId("ClientCommitFileUpload", appId, accountId)) {
        PB::Writer body;
        body.WriteVarint(1, 0);
        return body;
    }
    if (transferSucceeded) {
        // the file was PUT to HttpServer's blob store already -- verify it exists
        if (HttpServer::HasBlob(accountId, appId, cleanName)) {
            committed = true;

            // Read blob for cloud upload. No SHA re-verification: Steam sent the
            // data over localhost TCP and told us the transfer succeeded. Re-reading
            // from disk and re-hashing is redundant -- the only way the SHA could
            // differ is disk corruption in the milliseconds between PUT and Commit.
            // The real Steam server also doesn't re-verify SHA at commit time; it
            // trusts the uploaded data. Removing this check fixes spurious commit
            // rejections for volatile files (e.g. Player.log).
            auto blobData = HttpServer::ReadBlob(accountId, appId, cleanName);
            LOG("[NS-UP]   committed: %s (%zu bytes)", cleanName.c_str(), blobData.size());

            {
                // CloudStorage::StoreBlob writes to the local cache (same dir as
                // LocalStorage) and enqueues a cloud upload. Empty files are valid
                // and must be preserved as zero-byte blobs.
                const uint8_t* blobPtr = blobData.empty() ? nullptr : blobData.data();
                if (!CloudStorage::StoreBlob(accountId, appId, cleanName,
                        blobPtr, blobData.size())) {
                    LOG("[NS-UP]   ERROR: failed to store blob for %s", cleanName.c_str());
                    committed = false;
                }
            }

            // Record which root token this file was uploaded under.
            // The changelist will only present files under their upload token,
            // preventing Steam's rootoverrides from seeing cross-platform
            // duplicates and issuing spurious deletes.
            std::string rootToken = ExtractRootToken(filename);
            PrepareBatchCanonicalTokens(accountId, appId);
            rootToken = CanonicalizeUploadRootToken(accountId, appId, cleanName, rootToken);
            RecordFileToken(accountId, appId, cleanName, rootToken);
            MarkFileTokensDirty(accountId, appId);
        } else {
            LOG("[NS-UP]   WARNING: blob not found after PUT for %s (clean=%s)", filename.c_str(), cleanName.c_str());
        }

        // Cloud upload already enqueued by CloudStorage::StoreBlob above
    }

    PB::Writer body;
    body.WriteVarint(1, committed ? 1 : 0);          // file_committed
    return body;
}

// CompleteAppUploadBatchBlocking
PB::Writer HandleCompleteBatch(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    // Persist file tokens once per batch (deferred from per-file commits/deletes).
    // This replaces N redundant file_tokens.dat cloud uploads with a single one.
    {
        std::unordered_set<uint64_t> dirtyApps;
        {
            std::lock_guard<std::mutex> lock(g_fileTokensDirtyMutex);
            dirtyApps.swap(g_fileTokensDirtyApps);
        }
        for (uint64_t dirty : dirtyApps) {
            uint32_t dirtyAccountId = static_cast<uint32_t>(dirty >> 32);
            uint32_t dirtyAppId = static_cast<uint32_t>(dirty & 0xFFFFFFFFu);
            PersistFileTokens(dirtyAccountId, dirtyAppId);
        }
    }

    // Increment CN once per batch (not per file) to match real Steam behavior.
    // This prevents the CN from climbing rapidly and causing conflict dialogs
    // when Steam restarts with clientCN=0.
    uint32_t accountId = 0;
    if (!RequireAccountId("CompleteAppUploadBatchBlocking", appId, accountId)) {
        return PB::Writer();
    }
    // Advance CN to stay consistent with the local writes already committed
    // by HandleCommitFileUpload / PersistFileTokens. CommitCNWithRetry drains
    // pending blob/token work, publishes CN, and on failure async-retries +
    // blocks on a second drain so process exit does not drop the publish and
    // failed deletes do not resurrect on the next SyncFromCloud.
    uint64_t newCN = LocalStorage::IncrementChangeNumber(accountId, appId);
    bool committed = CloudStorage::CommitCNWithRetry(accountId, appId, newCN);
    LOG("[NS] CompleteBatch app=%u CN=%llu committed=%d", appId, newCN, committed ? 1 : 0);
    ClearBatchCanonicalTokens(accountId, appId);
    PB::Writer body; // empty response
    return body;
}

// ClientFileDownload
// Tell Steam to GET the file from our local HTTP server.
PB::Writer HandleFileDownload(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    std::string filename;
    for (auto& f : reqBody) {
        if (f.fieldNum == 2 && f.wireType == PB::LengthDelimited)
            filename.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
    }

    uint32_t accountId = 0;
    if (!RequireAccountId("ClientFileDownload", appId, accountId)) {
        return PB::Writer();
    }
    uint16_t port = HttpServer::GetPort();
    std::string urlHost = "127.0.0.1:" + std::to_string(port);
    std::string cleanName = StripRootToken(filename);
    std::string urlPath = "/download/" + std::to_string(accountId) + "/" + std::to_string(appId)
        + "/" + HttpUtil::UrlEncode(cleanName, true);

    // get file metadata from local storage (single-file lookup, no full dir scan)
    uint64_t fileSize = 0;    uint64_t timestamp = 0;
    std::vector<uint8_t> sha;

    auto entry = LocalStorage::GetFileEntry(accountId, appId, cleanName);
    if (entry) {
        fileSize = entry->rawSize;
        timestamp = entry->timestamp;
        sha = entry->sha;
    }

    // fall back to blob store if not in local storage metadata
    if (fileSize == 0) {
        fileSize = HttpServer::GetBlobSize(accountId, appId, cleanName);
    }

    LOG("[NS-DL] FileDownload app=%u file=%s (clean=%s) size=%llu -> %s%s",
        appId, filename.c_str(), cleanName.c_str(), fileSize, urlHost.c_str(), urlPath.c_str());

    PB::Writer body;
    body.WriteVarint(1, appId);                      // appid
    body.WriteVarint(2, fileSize);                   // file_size (compressed = same)
    body.WriteVarint(3, fileSize);                   // raw_file_size
    if (!sha.empty())
        body.WriteBytes(4, sha.data(), sha.size());  // sha_file
    body.WriteVarint(5, timestamp);                  // time_stamp
    body.WriteVarint(6, 0);                          // is_explicit_delete = false
    body.WriteString(7, urlHost);                    // url_host
    body.WriteString(8, urlPath);                    // url_path
    body.WriteVarint(9, 0);                          // use_https = false
    // no request_headers (field 10)
    body.WriteVarint(11, 0);                         // encrypted = false

    return body;
}

// ClientDeleteFile
PB::Writer HandleDeleteFile(uint32_t appId, const std::vector<PB::Field>& reqBody) {
    std::string filename;
    for (auto& f : reqBody) {
        if (f.fieldNum == 2 && f.wireType == PB::LengthDelimited)
            filename.assign(reinterpret_cast<const char*>(f.data), f.dataLen);
    }

    std::string cleanName = StripRootToken(filename);
    LOG("[NS] DeleteFile app=%u file=%s (clean=%s)", appId, filename.c_str(), cleanName.c_str());

    if (IsInternalMetadataFile(cleanName)) {
        LOG("[NS] DeleteFile app=%u ignored for internal metadata %s", appId, cleanName.c_str());
        return PB::Writer();
    }

    uint32_t accountId = 0;
    if (!RequireAccountId("ClientDeleteFile", appId, accountId)) {
        return PB::Writer();
    }
    // CloudStorage::DeleteBlob removes from local cache and cloud, and
    // increments the change number so Steam re-downloads the file list.
    HttpServer::DeleteBlob(accountId, appId, cleanName);

    // Delete from cloud provider (async -- enqueues delete if provider active)
    CloudStorage::DeleteBlob(accountId, appId, cleanName);

    // Remove file-token mapping and mark dirty (persist deferred to CompleteBatch)
    RemoveFileToken(accountId, appId, cleanName);
    MarkFileTokensDirty(accountId, appId);

    PB::Writer body; // empty response
    return body;
}

} // namespace CloudIntercept
