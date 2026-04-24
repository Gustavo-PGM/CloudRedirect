#include "cloud_storage.h"
#include "local_storage.h"
#include "local_disk_provider.h"
#include "google_drive_provider.h"
#include "onedrive_provider.h"
#include "cloud_intercept.h"
#include "file_util.h"
#include "log.h"
#include <fstream>
#include <filesystem>
#include <sstream>
#include <chrono>
#include <list>
#include <algorithm>
#include <limits>
#include <Windows.h>

namespace CloudStorage {

static std::string CanonicalizeInternalMetadataName(std::string_view filename) {
    if (filename == CloudIntercept::kLegacyPlaytimeMetadataPath) {
        return CloudIntercept::kPlaytimeMetadataPath;
    }
    if (filename == CloudIntercept::kLegacyStatsMetadataPath) {
        return CloudIntercept::kStatsMetadataPath;
    }
    return std::string(filename);
}


static std::string                       g_localRoot;     // local cache root (e.g. "C:\Games\Steam\cloud_redirect\")
static std::unique_ptr<ICloudProvider>   g_provider;      // may be nullptr (local-only mode)
static std::mutex                        g_mutex;

// Non-blocking dialog when cloud operations fail repeatedly.
// Shows once after FAIL_THRESHOLD consecutive failures, then
// suppresses for COOLDOWN_SECS to avoid spamming.

static constexpr int    FAIL_THRESHOLD   = 3;
static constexpr int    COOLDOWN_SECS    = 300; // 5 minutes

static std::atomic<int> g_consecutiveFails{0};
static std::atomic<int64_t> g_lastDialogTime{0};
static std::mutex g_dialogMutex;
static std::thread g_dialogThread;

static void ShowCloudError(const std::string& message) {
    // check cooldown
    int64_t now = (int64_t)time(nullptr);
    int64_t last = g_lastDialogTime.load();
    if (last > 0 && now - last < COOLDOWN_SECS) return;
    g_lastDialogTime.store(now);

    LOG("[CloudStorage] Showing error dialog: %s", message.c_str());

    // non-blocking: fire-and-forget thread for MessageBox
    // Tracked so Shutdown can join before DLL unload.
    std::string msg = message; // copy for thread
    std::lock_guard<std::mutex> lock(g_dialogMutex);
    if (g_dialogThread.joinable()) g_dialogThread.join();
    g_dialogThread = std::thread([msg]() {
        MessageBoxA(nullptr, msg.c_str(), "CloudRedirect - Cloud Sync Error",
                    MB_OK | MB_ICONWARNING | MB_SYSTEMMODAL);
    });
}

// Call after a cloud operation fails. Shows dialog after N consecutive failures.
static void OnCloudFailure(const char* operation, const std::string& path) {
    int fails = ++g_consecutiveFails;
    if (fails == FAIL_THRESHOLD) {
        std::string provName = g_provider ? g_provider->Name() : "Cloud";
        ShowCloudError(
            provName + " sync error: " + std::string(operation) +
            " has failed " + std::to_string(fails) + " times.\n\n"
            "Your saves may not be syncing to the cloud.\n"
            "Check your internet connection and cloud_redirect.log for details.\n\n"
            "Last failed path: " + path);
    }
}

static void OnCloudSuccess() {
    g_consecutiveFails.store(0);
}

// Show an immediate dialog for critical auth failures (token refresh broken).
void NotifyAuthFailure(const std::string& providerName) {
    ShowCloudError(
        providerName + " authentication failed!\n\n"
        "CloudRedirect cannot refresh your access token.\n"
        "Cloud sync is disabled until this is resolved.\n\n"
        "Re-authenticate using the CloudRedirect setup tool.");
}

// background work queue
struct WorkItem {
    enum Type { Upload, Delete };
    Type        type;
    std::string cloudPath;          // relative path for provider
    std::vector<uint8_t> data;      // only for Upload
    bool        skipIfExists = false;
    int         existsCheckRetries = 0;  // Upload-only: retries of CheckExists before skipIfExists upload
    int         transferRetries = 0;     // Upload/Delete: retries of the actual Upload/Remove call
    int         drainRequeues = 0;       // Times this item was requeued via RequeueFailedWorkForPrefixLocked
};

static std::list<WorkItem>               g_workQueue;
// LOCK ORDER: this is a SINK in the AutoCloud DAG — never acquired while
// any CloudRedirect mutex is already held (except transitively from
// g_autoCloudImportMutex via EnqueueWork from the import critical region).
// INVARIANT: never held across a provider call (network I/O) or across
// any LocalStorage::* call. The worker drops this mutex between dequeue
// and the provider call, and between the provider call and bookkeeping.
// Collapsing those two disjoint scopes would serialize the worker pool
// on network latency. LocalStorage::ClearDeleted in the worker's Delete
// path runs in the unlocked window between the two scopes.
static std::mutex                        g_queueMutex;
static std::condition_variable           g_queueCV;
// O(1) dedup index: maps cloudPath -> iterator into g_workQueue for Upload items.
// Replaces the O(n) linear scan that previously checked every queued item (H8).
static std::unordered_map<std::string, std::list<WorkItem>::iterator> g_uploadIndex;
static std::vector<std::thread>          g_workerThreads;
static std::atomic<bool>                 g_workerRunning{false};
static std::atomic<int>                  g_activeWorkers{0};
static std::unordered_map<std::string, int> g_activePaths;
// g_failedPaths tracks ONLY retriable failures: items that DrainQueueForApp
// should still wait on and callers should treat as unfinished work. Once an
// item exhausts MAX_DRAIN_REQUEUES it is removed from this set even though it
// stays in g_failedWorkItems for diagnostic purposes, so that callers like
// CommitCNWithRetry stop blocking for 30s per drain on work we have given up
// on.
static std::unordered_set<std::string>   g_failedPaths;
// g_failedWorkItems is the dead-letter record of every failed item, including
// poisoned ones. It drives RequeueFailedWorkForPrefixLocked's decision on
// whether to retry or skip, and is cleared when a fresh EnqueueWork supersedes
// the failed item or at Shutdown.
static std::unordered_map<std::string, WorkItem> g_failedWorkItems;
static std::condition_variable           g_drainCV;       // signaled when a worker finishes an item
static constexpr int                     WORKER_THREAD_COUNT = 4;

static bool HasPendingWorkForPrefix(const std::string& prefix) {
    for (const auto& item : g_workQueue) {
        if (item.cloudPath.rfind(prefix, 0) == 0) return true;
    }
    for (const auto& [path, count] : g_activePaths) {
        if (count > 0 && path.rfind(prefix, 0) == 0) return true;
    }
    return false;
}

static bool HasFailedWorkForPrefix(const std::string& prefix) {
    for (const auto& path : g_failedPaths) {
        if (path.rfind(prefix, 0) == 0) return true;
    }
    return false;
}

static void ClearFailedWorkForPrefix(const std::string& prefix) {
    for (auto it = g_failedPaths.begin(); it != g_failedPaths.end(); ) {
        if (it->rfind(prefix, 0) == 0) it = g_failedPaths.erase(it);
        else ++it;
    }
}

// Cap the number of times a single failed item can be requeued by repeated
// DrainQueueForApp calls, so a permanently-broken provider (e.g. bad OAuth
// token, rejected filename) does not get retried indefinitely.
static constexpr int MAX_DRAIN_REQUEUES = 3;

static void RequeueFailedWorkForPrefixLocked(const std::string& prefix) {
    for (auto it = g_failedWorkItems.begin(); it != g_failedWorkItems.end(); ) {
        if (it->first.rfind(prefix, 0) != 0) {
            ++it;
            continue;
        }
        if (it->second.drainRequeues >= MAX_DRAIN_REQUEUES) {
            // Poison item: exhausted both inline retries and drain requeues.
            // Drop it from g_failedPaths so HasFailedWorkForPrefix stops
            // reporting the failure to drain callers (otherwise CommitCNWithRetry
            // stalls Steam's RPC thread ~60s per batch for the lifetime of the
            // poison). Keep it in g_failedWorkItems so this same item cannot be
            // silently re-queued by a future drain; a fresh EnqueueWork for the
            // same cloudPath will clear both maps and grant a clean attempt.
            g_failedPaths.erase(it->first);
            ++it;
            continue;
        }
        WorkItem item = std::move(it->second);
        // Grant a fresh inline-retry budget for this drain attempt while
        // preserving the drain-requeue counter so unbounded retries are
        // not possible across repeated drains.
        item.existsCheckRetries = 0;
        item.transferRetries = 0;
        ++item.drainRequeues;
        g_failedPaths.erase(it->first);
        if (!g_activePaths.count(item.cloudPath)) {
            g_workQueue.push_back(std::move(item));
            auto queued = std::prev(g_workQueue.end());
            if (queued->type == WorkItem::Upload) {
                g_uploadIndex[queued->cloudPath] = queued;
            }
        }
        it = g_failedWorkItems.erase(it);
    }
}

static void EnqueueWork(WorkItem item);
// Worker-only retry path: re-queue an item that just failed, but only if no
// fresher upload for the same cloudPath is already queued. Returns true if
// the item was queued (and caller should treat it as "requeued"); returns
// false if a fresher caller-enqueued upload already supersedes this retry
// and the caller should drop it on the floor.
//
// This exists because EnqueueWork's dedup path moves the new item's data
// INTO the existing queued entry. If a worker calls EnqueueWork to retry a
// stale Upload while a fresher Upload is already queued for the same path,
// dedup would overwrite the fresher bytes with the stale ones and silently
// lose saves. Callers (fresh Steam enqueues) want that last-writer-wins
// semantics; workers retrying stale bytes do not.
static bool RequeueFromWorker(WorkItem item);
static std::string LocalStoragePath(uint32_t accountId, uint32_t appId);

static std::string CreateLocalConflictCopy(uint32_t accountId, uint32_t appId,
                                           const std::string& filename,
                                           const std::string& localPath) {
    std::string conflictsRoot = g_localRoot + "conflicts\\";
    std::string appConflictRoot = conflictsRoot + std::to_string(accountId) + "\\" +
        std::to_string(appId) + "\\";
    auto stamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::string conflictPath = appConflictRoot + filename + "." + std::to_string(stamp) + ".local";
    for (auto& c : conflictPath) { if (c == '/') c = '\\'; }
    std::error_code ec;
    std::filesystem::create_directories(appConflictRoot, ec);
    if (ec) return {};
    if (!FileUtil::IsPathWithin(appConflictRoot, conflictPath)) return {};

    std::filesystem::create_directories(std::filesystem::path(conflictPath).parent_path(), ec);
    if (ec) return {};
    std::filesystem::copy_file(localPath, conflictPath,
        std::filesystem::copy_options::none, ec);
    if (!ec) {
        LOG("[CloudStorage] Preserved local conflict copy for app %u file %s at %s",
            appId, filename.c_str(), conflictPath.c_str());
        return conflictPath;
    }
    LOG("[CloudStorage] Failed to preserve local conflict copy for app %u file %s: %s",
        appId, filename.c_str(), ec.message().c_str());
    return {};
}

static bool PreserveLocalConflictCopy(uint32_t accountId, uint32_t appId,
                                      const std::string& filename,
                                      const std::string& localPath) {
    return !CreateLocalConflictCopy(accountId, appId, filename, localPath).empty();
}

static void RemoveLocalBlobsNotInCloud(uint32_t accountId, uint32_t appId,
                                       const std::unordered_set<std::string>& cloudBlobNames) {
    std::string localBlobDir = LocalStoragePath(accountId, appId);
    std::error_code ec;
    if (!std::filesystem::exists(localBlobDir, ec) || !std::filesystem::is_directory(localBlobDir, ec)) return;

    int removed = 0;
    for (auto& entry : std::filesystem::recursive_directory_iterator(localBlobDir, ec)) {
        if (ec) break;
        if (!entry.is_regular_file(ec)) continue;

        std::string rel = std::filesystem::relative(entry.path(), localBlobDir, ec).string();
        if (ec) continue;
        for (auto& c : rel) { if (c == '\\') c = '/'; }
        if (rel == "cn.dat" || rel == "root_token.dat" ||
            rel == "file_tokens.dat" || rel == "deleted.dat") continue;
        if (cloudBlobNames.count(rel)) continue;

        if (!PreserveLocalConflictCopy(accountId, appId, rel, entry.path().string())) continue;
        std::filesystem::remove(entry.path(), ec);
        if (!ec) ++removed;
    }
    if (removed > 0) {
        LOG("[CloudStorage] SyncFromCloud app %u: removed %d stale local blob(s) absent from newer cloud CN",
            appId, removed);
    }
}


// Cloud provider paths use forward slashes: "{accountId}/{appId}/blobs/{filename}"
static std::string CloudBlobPath(uint32_t accountId, uint32_t appId,
                                 const std::string& filename) {
    return std::to_string(accountId) + "/" + std::to_string(appId) +
           "/blobs/" + filename;
}

// Inverse of CloudBlobPath — returns false if the path doesn't match the
// "{accountId}/{appId}/blobs/{filename}" shape. Only blob paths are parsable;
// metadata paths (cn.dat, root_token.dat, etc.) deliberately fail here.
//
// Parses only canonical unsigned decimal for accountId/appId — rejects signs,
// whitespace, leading '+' or '-', and values that don't fit in uint32_t. This
// keeps the parser symmetric with CloudBlobPath (which always writes canonical
// unsigned decimal) so a round-trip never produces a different (acct, app)
// pair than the one we started with.
static bool ParseCloudBlobPath(const std::string& cloudPath,
                               uint32_t& accountId, uint32_t& appId,
                               std::string& filename) {
    size_t p1 = cloudPath.find('/');
    if (p1 == std::string::npos || p1 == 0) return false;
    size_t p2 = cloudPath.find('/', p1 + 1);
    if (p2 == std::string::npos || p2 == p1 + 1) return false;
    const std::string kBlobs = "/blobs/";
    if (cloudPath.compare(p2, kBlobs.size(), kBlobs) != 0) return false;
    size_t fileStart = p2 + kBlobs.size();
    if (fileStart >= cloudPath.size()) return false;

    auto parseU32 = [](const std::string& s, uint32_t& out) -> bool {
        if (s.empty()) return false;
        for (char c : s) {
            if (c < '0' || c > '9') return false; // no sign, whitespace, or letters
        }
        try {
            unsigned long long v = std::stoull(s);
            if (v > (std::numeric_limits<uint32_t>::max)()) return false;
            out = static_cast<uint32_t>(v);
            return true;
        } catch (...) { return false; }
    };

    if (!parseU32(cloudPath.substr(0, p1), accountId)) return false;
    if (!parseU32(cloudPath.substr(p1 + 1, p2 - p1 - 1), appId)) return false;
    filename = cloudPath.substr(fileStart);
    return !filename.empty();
}

static std::string CloudMetadataPath(uint32_t accountId, uint32_t appId,
                                     const std::string& name) {
    return std::to_string(accountId) + "/" + std::to_string(appId) + "/" + name;
}

// Local cache paths use the existing LocalStorage layout:
//   {g_localRoot}\storage\{accountId}\{appId}\{filename}  — all file data + metadata
// Previously blobs were stored separately in a "blobs\" directory, but this caused
// desync between changelist metadata and HTTP-served file bytes. Now unified.
static std::string LocalStoragePath(uint32_t accountId, uint32_t appId) {
    return g_localRoot + "storage\\" + std::to_string(accountId) + "\\" +
           std::to_string(appId) + "\\";
}

static std::unordered_set<uint32_t> EnumerateLocalAppIds(uint32_t accountId) {
    std::unordered_set<uint32_t> appIds;
    std::string accountRoot = g_localRoot + "storage\\" + std::to_string(accountId) + "\\";
    std::error_code ec;
    if (!std::filesystem::exists(accountRoot, ec) || !std::filesystem::is_directory(accountRoot, ec)) {
        return appIds;
    }

    for (const auto& entry : std::filesystem::directory_iterator(accountRoot, ec)) {
        if (ec) break;
        if (!entry.is_directory(ec)) continue;
        const std::string name = entry.path().filename().string();
        try {
            appIds.insert(static_cast<uint32_t>(std::stoul(name)));
        } catch (...) {
        }
    }
    return appIds;
}

static std::string LocalBlobPath(uint32_t accountId, uint32_t appId,
                                 const std::string& filename) {
    std::string path = g_localRoot + "storage\\" + std::to_string(accountId) +
                       "\\" + std::to_string(appId) + "\\" + filename;
    for (auto& c : path) { if (c == '/') c = '\\'; }

    std::string storageRoot = g_localRoot + "storage\\";
    if (!FileUtil::IsPathWithin(storageRoot, path)) {
        LOG("[CloudStorage] BLOCKED path traversal: '%s' root='%s'",
            filename.c_str(), storageRoot.c_str());
        return {};
    }

    return path;
}


static void WorkerLoop(int threadId) {
    LOG("[CloudStorage] Background worker %d started", threadId);
    int consecutiveFailures = 0;
    while (g_workerRunning) {
        WorkItem item;
        {
            std::unique_lock<std::mutex> lock(g_queueMutex);
            g_queueCV.wait(lock, [] {
                if (!g_workerRunning) return true;
                for (const auto& queued : g_workQueue) {
                    if (!g_activePaths.count(queued.cloudPath)) return true;
                }
                return false;
            });
            if (!g_workerRunning && g_workQueue.empty()) break;

            auto workIt = std::find_if(g_workQueue.begin(), g_workQueue.end(),
                [](const WorkItem& queued) {
                    return !g_activePaths.count(queued.cloudPath);
                });
            if (workIt == g_workQueue.end()) continue;

            item = std::move(*workIt);
            // Remove from dedup index before popping (H8)
            if (item.type == WorkItem::Upload) {
                g_uploadIndex.erase(item.cloudPath);
            }
            g_workQueue.erase(workIt);
            ++g_activeWorkers;
            ++g_activePaths[item.cloudPath];
        }

        if (!g_provider) { --g_activeWorkers; g_drainCV.notify_all(); continue; }

        // Exponential backoff after consecutive failures (cap at 30s)
        if (consecutiveFailures > 0) {
            int delayMs = 1000 * (1 << (consecutiveFailures < 5 ? consecutiveFailures : 5));
            if (delayMs > 30000) delayMs = 30000;
            LOG("[CloudStorage] Worker %d backing off %d ms after %d consecutive failure(s)",
                threadId, delayMs, consecutiveFailures);
            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        }

        std::string activePath = item.cloudPath;
        bool success = false;
        bool requeued = false;
        bool droppedAsStale = false;
        switch (item.type) {
        case WorkItem::Upload:
            if (item.skipIfExists) {
                auto exists = g_provider->CheckExists(item.cloudPath);
                if (exists == ICloudProvider::ExistsStatus::Exists) {
                    LOG("[CloudStorage] BG upload skipped existing [%d]: %s",
                        threadId, item.cloudPath.c_str());
                    OnCloudSuccess();
                    success = true;
                    break;
                }
                if (exists == ICloudProvider::ExistsStatus::Error && item.existsCheckRetries++ < 3) {
                    LOG("[CloudStorage] BG upload deferred after existence check failure [%d]: %s",
                        threadId, item.cloudPath.c_str());
                    OnCloudFailure("Exists", item.cloudPath);
                    requeued = RequeueFromWorker(std::move(item));
                    if (!requeued) droppedAsStale = true;
                    break;
                }
                if (exists == ICloudProvider::ExistsStatus::Error) {
                    LOG("[CloudStorage] BG upload abandoned after repeated existence check failures [%d]: %s",
                        threadId, item.cloudPath.c_str());
                    OnCloudFailure("Exists", item.cloudPath);
                    break;
                }
            }
            if (g_provider->Upload(item.cloudPath, item.data.data(), item.data.size())) {
                LOG("[CloudStorage] BG upload OK [%d]: %s (%zu bytes)",
                    threadId, item.cloudPath.c_str(), item.data.size());
                OnCloudSuccess();
                success = true;
            } else {
                LOG("[CloudStorage] BG upload FAILED [%d]: %s", threadId, item.cloudPath.c_str());
                OnCloudFailure("Upload", item.cloudPath);
                if (item.transferRetries++ < 3) {
                    requeued = RequeueFromWorker(std::move(item));
                    if (!requeued) droppedAsStale = true;
                }
            }
            break;
        case WorkItem::Delete:
            if (g_provider->Remove(item.cloudPath)) {
                LOG("[CloudStorage] BG delete OK [%d]: %s", threadId, item.cloudPath.c_str());
                OnCloudSuccess();
                success = true;
                // Cloud confirmed the delete — drop the local tombstone so a
                // future StoreBlob of the same filename isn't treated as a
                // resurrection candidate. Parsing failure (e.g. metadata path)
                // is silent: non-blob deletes have no tombstone to clear.
                {
                    uint32_t doneAcct = 0, doneApp = 0;
                    std::string doneFile;
                    if (ParseCloudBlobPath(item.cloudPath, doneAcct, doneApp, doneFile)) {
                        LocalStorage::ClearDeleted(doneAcct, doneApp, doneFile);
                    }
                }
            } else {
                LOG("[CloudStorage] BG delete FAILED [%d]: %s", threadId, item.cloudPath.c_str());
                OnCloudFailure("Delete", item.cloudPath);
                if (item.transferRetries++ < 3) {
                    requeued = RequeueFromWorker(std::move(item));
                    if (!requeued) droppedAsStale = true;
                }
            }
            break;
        }

        if (success)
            consecutiveFailures = 0;
        else
            ++consecutiveFailures;

        {
            std::lock_guard<std::mutex> lock(g_queueMutex);
            auto it = g_activePaths.find(activePath);
            if (it != g_activePaths.end()) {
                if (--it->second <= 0) g_activePaths.erase(it);
            }
            if (success) {
                g_failedPaths.erase(activePath);
                g_failedWorkItems.erase(activePath);
            } else if (droppedAsStale) {
                // Fresher upload already supersedes this retry (handled by
                // RequeueFromWorker); do not record as failed — the new
                // queued item is authoritative.
            } else if (!requeued) {
                g_failedPaths.insert(activePath);
                g_failedWorkItems[activePath] = std::move(item);
            }
            --g_activeWorkers;
        }
        g_drainCV.notify_all();
        g_queueCV.notify_all();
    }
    LOG("[CloudStorage] Background worker %d stopped", threadId);
}

static void EnqueueWork(WorkItem item) {
    {
        std::lock_guard<std::mutex> lock(g_queueMutex);
        g_failedPaths.erase(item.cloudPath);
        g_failedWorkItems.erase(item.cloudPath);

        // Deduplication: for Upload items, if the same cloudPath is already
        // queued for upload, replace it with the newer data. This eliminates
        // redundant uploads of metadata files (cn.dat, file_tokens.dat, etc.)
        // that get re-enqueued multiple times during a batch.
        // Uses O(1) index lookup instead of O(n) queue scan (H8).
        if (item.type == WorkItem::Upload) {
            auto indexIt = g_uploadIndex.find(item.cloudPath);
            if (indexIt != g_uploadIndex.end()) {
                auto& existing = *indexIt->second;
                if (item.skipIfExists && !existing.skipIfExists) {
                    LOG("[CloudStorage] Dedup: keeping queued authoritative upload for %s",
                        item.cloudPath.c_str());
                    return;
                }
                LOG("[CloudStorage] Dedup: replacing queued upload for %s (%zu -> %zu bytes)",
                    item.cloudPath.c_str(), existing.data.size(), item.data.size());
                existing.data = std::move(item.data);
                existing.skipIfExists = item.skipIfExists;
                // Fresh data supersedes retry history: new caller-level intent
                // should reset the inline-retry budget. Preserve drainRequeues
                // so poison paths still hit the overall cap.
                existing.existsCheckRetries = item.existsCheckRetries;
                existing.transferRetries = item.transferRetries;
                return; // replaced in-place, no need to notify
            }
        }

        g_workQueue.push_back(std::move(item));
        auto it = std::prev(g_workQueue.end());
        if (it->type == WorkItem::Upload) {
            g_uploadIndex[it->cloudPath] = it;
        }
    }
    g_queueCV.notify_one();
}

static bool RequeueFromWorker(WorkItem item) {
    std::lock_guard<std::mutex> lock(g_queueMutex);
    // A fresher caller-enqueued Upload for the same path supersedes this
    // retry; drop the retry so we do not clobber fresher bytes.
    if (item.type == WorkItem::Upload) {
        auto indexIt = g_uploadIndex.find(item.cloudPath);
        if (indexIt != g_uploadIndex.end()) {
            LOG("[CloudStorage] Retry dropped: newer upload already queued for %s",
                item.cloudPath.c_str());
            // Clear any failure state for this path so the newer queued item
            // is the sole authority.
            g_failedPaths.erase(item.cloudPath);
            g_failedWorkItems.erase(item.cloudPath);
            return false;
        }
    }
    // For Delete retries: if an Upload for the same path is now queued, the
    // user has overwritten the file; the delete is stale. Drop it so we do
    // not nuke the newly-uploaded bytes.
    if (item.type == WorkItem::Delete) {
        auto indexIt = g_uploadIndex.find(item.cloudPath);
        if (indexIt != g_uploadIndex.end()) {
            LOG("[CloudStorage] Retry dropped: upload supersedes stale delete for %s",
                item.cloudPath.c_str());
            g_failedPaths.erase(item.cloudPath);
            g_failedWorkItems.erase(item.cloudPath);
            return false;
        }
    }
    // Clear prior failure state; this retry is now the pending attempt.
    g_failedPaths.erase(item.cloudPath);
    g_failedWorkItems.erase(item.cloudPath);
    g_workQueue.push_back(std::move(item));
    auto qit = std::prev(g_workQueue.end());
    if (qit->type == WorkItem::Upload) {
        g_uploadIndex[qit->cloudPath] = qit;
    }
    g_queueCV.notify_one();
    return true;
}

// Enqueue a cloud upload of the current CN value for this app.
// Dedup in EnqueueWork will coalesce multiple calls during a batch.
void PushCNToCloud(uint32_t accountId, uint32_t appId, uint64_t cn) {
    std::string cnStr = std::to_string(cn);
    WorkItem wi;
    wi.type = WorkItem::Upload;
    wi.cloudPath = CloudMetadataPath(accountId, appId, "cn.dat");
    wi.data.assign(cnStr.begin(), cnStr.end());
    EnqueueWork(std::move(wi));
}

bool PushCNToCloudSync(uint32_t accountId, uint32_t appId, uint64_t cn) {
    if (!g_provider) return true;
    std::string cnStr = std::to_string(cn);
    std::string cloudPath = CloudMetadataPath(accountId, appId, "cn.dat");
    return g_provider->Upload(cloudPath, reinterpret_cast<const uint8_t*>(cnStr.data()), cnStr.size());
}

bool CommitCNWithRetry(uint32_t accountId, uint32_t appId, uint64_t cn) {
    // DrainQueueForApp only waits on retriable pending work; it reports
    // drained=true once the only remaining failed items are poisoned
    // (MAX_DRAIN_REQUEUES exhausted). That is the right behavior here
    // because poisoned items will never succeed and we should not stall
    // Steam's hot-path RPC thread waiting for them.
    bool drained = DrainQueueForApp(accountId, appId);
    bool cnPublished = drained && PushCNToCloudSync(accountId, appId, cn);
    if (cnPublished) return true;
    // Genuine transient failure (drain timed out or sync CN push failed).
    // Queue an async retry so the CN publish survives process exit, then
    // block on a second drain so failed Uploads/Deletes for this app prefix
    // get one more chance before this batch returns to Steam — a failed
    // cloud delete left behind can resurrect on the next SyncFromCloud.
    LOG("[CloudStorage] CommitCNWithRetry app %u CN=%llu drained=%d: deferring to async retry",
        appId, (unsigned long long)cn, drained ? 1 : 0);
    PushCNToCloud(accountId, appId, cn);
    DrainQueueForApp(accountId, appId);
    return false;
}


void Init(const std::string& localRoot, std::unique_ptr<ICloudProvider> provider) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_localRoot = localRoot;
    if (!g_localRoot.empty() && g_localRoot.back() != '\\')
        g_localRoot += '\\';

    g_provider = std::move(provider);

    LOG("[CloudStorage] Initialized. localRoot=%s provider=%s",
        g_localRoot.c_str(), g_provider ? g_provider->Name() : "none (local-only)");

    // Start background workers if we have a cloud provider
    if (g_provider) {
        g_workerRunning = true;
        for (int i = 0; i < WORKER_THREAD_COUNT; ++i) {
            g_workerThreads.emplace_back(WorkerLoop, i);
        }
        LOG("[CloudStorage] Started %d background worker threads", WORKER_THREAD_COUNT);
    }
}

void Shutdown() {
    LOG("[CloudStorage] Shutting down...");

    // Signal workers to stop
    g_workerRunning = false;
    g_queueCV.notify_all();

    for (auto& t : g_workerThreads) {
        if (t.joinable()) t.join();
    }
    g_workerThreads.clear();

    // Clear any remaining queued items and dedup index, and drop failed-work
    // state so a fresh Init() doesn't inherit stale entries or keep payload
    // bytes alive for apps the user will never touch again.
    {
        std::lock_guard<std::mutex> lock(g_queueMutex);
        g_workQueue.clear();
        g_uploadIndex.clear();
        g_failedPaths.clear();
        g_failedWorkItems.clear();
    }

    if (g_provider) {
        g_provider->Shutdown();
        g_provider.reset();
    }

    // Join any outstanding error dialog thread before DLL unload
    {
        std::lock_guard<std::mutex> lock(g_dialogMutex);
        if (g_dialogThread.joinable()) g_dialogThread.join();
    }

    LOG("[CloudStorage] Shutdown complete");
}

const char* ProviderName() {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (g_provider) return g_provider->Name();
    return "Local Only";
}

bool IsCloudActive() {
    std::lock_guard<std::mutex> lock(g_mutex);
    return g_provider && g_provider->IsAuthenticated();
}


bool StoreBlob(uint32_t accountId, uint32_t appId,
               const std::string& filename,
               const uint8_t* data, size_t len) {
    // 1. Write to local cache (synchronous — the HTTP server needs this immediately).
    //
    // Route through LocalStorage::WriteFileNoIncrement so the write is serialized
    // against SyncFromCloud's staged-blob promotion and RestoreFileIfUnchanged
    // rollback under LocalStorage's g_mutex. Without this, two concurrent writers
    // (game save via HTTP StoreBlob + cloud pull promoting a staged blob) race at
    // the filesystem-rename layer and RestoreFileIfUnchanged's compare-and-restore
    // cannot reliably detect the collision, risking silent save loss.
    if (!LocalStorage::WriteFileNoIncrement(accountId, appId, filename, data, len)) {
        LOG("[CloudStorage] StoreBlob: local write failed: app=%u file=%s (%zu bytes)",
            appId, filename.c_str(), len);
        return false;
    }
    LOG("[CloudStorage] StoreBlob: cached locally: %s (%zu bytes)", filename.c_str(), len);

    // User re-created the file — clear any stale tombstone so SyncFromCloud
    // won't later suppress it and so the background Upload below is the
    // authoritative cloud state for this filename.
    LocalStorage::ClearDeleted(accountId, appId, filename);

    // CN is incremented once per batch in HandleCompleteBatch, not per file.

    // 2. Enqueue async upload to cloud provider
    if (g_provider) {
        WorkItem wi;
        wi.type = WorkItem::Upload;
        wi.cloudPath = CloudBlobPath(accountId, appId, filename);
        if (len != 0) {
            wi.data.assign(data, data + len);
        }
        EnqueueWork(std::move(wi));
    }

    return true;
}

std::vector<uint8_t> RetrieveBlob(uint32_t accountId, uint32_t appId,
                                  const std::string& filename) {
    // 1. Check local cache
    std::string localPath = LocalBlobPath(accountId, appId, filename);
    if (localPath.empty()) return {}; // path traversal blocked
    {
        std::ifstream f(localPath, std::ios::binary | std::ios::ate);
        if (f) {
            auto size = f.tellg();
            f.seekg(0, std::ios::beg);
            std::vector<uint8_t> data(static_cast<size_t>(size));
            f.read(reinterpret_cast<char*>(data.data()), size);
            LOG("[CloudStorage] RetrieveBlob: cache hit: %s (%zu bytes)",
                filename.c_str(), data.size());
            return data;
        }
    }

    // 2. Cache miss — pull from cloud provider (blocking)
    if (g_provider) {
        std::string cloudPath = CloudBlobPath(accountId, appId, filename);
        std::vector<uint8_t> data;
        if (g_provider->Download(cloudPath, data)) {
            LOG("[CloudStorage] RetrieveBlob: downloaded from cloud: %s (%zu bytes)",
                filename.c_str(), data.size());
            // Populate local cache for next time, serialized with StoreBlob /
            // SyncFromCloud promotion / RestoreFileIfUnchanged via g_mutex.
            const uint8_t* writeData = data.empty() ? nullptr : data.data();
            LocalStorage::WriteFileNoIncrement(accountId, appId, filename,
                                               writeData, data.size());
            return data;
        }
        LOG("[CloudStorage] RetrieveBlob: not found in cloud: %s", filename.c_str());
    }

    LOG("[CloudStorage] RetrieveBlob: not found anywhere: %s", filename.c_str());
    return {};
}

bool DeleteBlob(uint32_t accountId, uint32_t appId,
                const std::string& filename) {
    // 1. Delete from local cache
    std::string localPath = LocalBlobPath(accountId, appId, filename);
    if (localPath.empty()) return false; // path traversal blocked
    std::error_code ec;
    std::filesystem::remove(localPath, ec);

    LOG("[CloudStorage] DeleteBlob: removed local cache: %s", filename.c_str());

    // 2. Record tombstone BEFORE enqueuing cloud delete. If the worker
    //    succeeds we clear it there; if it fails permanently the tombstone
    //    keeps SyncFromCloud from resurrecting the file on next startup.
    //    Stamp with the current local CN so a cross-machine re-save whose
    //    cloud CN exceeds this one can still override the tombstone and
    //    preserve Steam's "newer CN wins" semantics.
    uint64_t cnAtDelete = LocalStorage::GetChangeNumber(accountId, appId);
    LocalStorage::MarkDeleted(accountId, appId, filename, cnAtDelete);

    // CN is incremented once per batch in HandleCompleteBatch, not per file.

    // 3. Enqueue cloud delete
    if (g_provider) {
        WorkItem wi;
        wi.type = WorkItem::Delete;
        wi.cloudPath = CloudBlobPath(accountId, appId, filename);
        EnqueueWork(std::move(wi));
    }

    return true;
}

bool BlobExists(uint32_t accountId, uint32_t appId,
                const std::string& filename) {
    return CheckBlobExists(accountId, appId, filename) == ICloudProvider::ExistsStatus::Exists;
}

ICloudProvider::ExistsStatus CheckBlobExists(uint32_t accountId, uint32_t appId,
                                             const std::string& filename) {
    // Check local cache first
    std::string localPath = LocalBlobPath(accountId, appId, filename);
    if (localPath.empty()) return ICloudProvider::ExistsStatus::Error;  // path traversal rejected
    // Use a single status() call with error_code to avoid TOCTOU and to
    // avoid throwing if the file is deleted between exists()/is_regular_file()
    // (some Windows builds surface that race as filesystem_error).
    std::error_code statEc;
    auto st = std::filesystem::status(localPath, statEc);
    if (!statEc && std::filesystem::is_regular_file(st))
        return ICloudProvider::ExistsStatus::Exists;

    // Check cloud
    if (g_provider) {
        std::string cloudPath = CloudBlobPath(accountId, appId, filename);
        return g_provider->CheckExists(cloudPath);
    }

    return ICloudProvider::ExistsStatus::Missing;
}


// Helper: read a small file from local storage path
static std::string ReadLocalText(const std::string& path) {
    std::ifstream f(path);
    if (!f) return "";
    return std::string(std::istreambuf_iterator<char>(f),
                       std::istreambuf_iterator<char>());
}

// Helper: write a small text file to local storage path (atomic via .tmp+rename)
static bool WriteLocalText(const std::string& path, const std::string& content) {
    auto parent = std::filesystem::path(path).parent_path();
    std::error_code ec;
    std::filesystem::create_directories(parent, ec);
    if (ec) {
        LOG("[CloudStorage] WriteLocalText: failed to create parent %s: %s",
            parent.string().c_str(), ec.message().c_str());
        return false;
    }
    return FileUtil::AtomicWriteText(path, content);
}

// SaveMetadata: removed — metadata.json was never created locally by any code path.
// With the blob→storage copy in SyncFromCloud, GetFileList() works on restore
// without a separate metadata file.

uint64_t GetChangeNumber(uint32_t accountId, uint32_t appId) {
    // Read from LocalStorage's cn.dat (the authoritative local CN)
    // CloudStorage::SyncFromCloud will have already reconciled with cloud CN
    std::string cnPath = LocalStoragePath(accountId, appId) + "cn.dat";
    std::string content = ReadLocalText(cnPath);
    if (!content.empty()) {
        try {
            return std::stoull(content);
        } catch (...) {}
    }
    return 1; // default
}


void SaveRootTokens(uint32_t accountId, uint32_t appId,
                    const std::unordered_set<std::string>& tokens) {
    // Delegate to LocalStorage for the actual disk write
    LocalStorage::SaveRootTokens(accountId, appId, tokens);

    // Push to cloud
    if (g_provider) {
        std::string content;
        for (auto& t : tokens) {
            content += t + "\n";
        }
        WorkItem wi;
        wi.type = WorkItem::Upload;
        wi.cloudPath = CloudMetadataPath(accountId, appId, "root_token.dat");
        wi.data.assign(content.begin(), content.end());
        EnqueueWork(std::move(wi));
    }
}

std::unordered_set<std::string> LoadRootTokens(uint32_t accountId, uint32_t appId) {
    // Just read from local — SyncFromCloud will have pulled the cloud version already
    return LocalStorage::LoadRootTokens(accountId, appId);
}

void SaveFileTokens(uint32_t accountId, uint32_t appId,
                    const std::unordered_map<std::string, std::string>& fileTokens) {
    // Delegate to LocalStorage for the actual disk write
    LocalStorage::SaveFileTokens(accountId, appId, fileTokens);

    // Push to cloud
    if (g_provider) {
        std::string content;
        for (auto& [cleanName, token] : fileTokens) {
            content += cleanName + "\t" + token + "\n";
        }
        WorkItem wi;
        wi.type = WorkItem::Upload;
        wi.cloudPath = CloudMetadataPath(accountId, appId, "file_tokens.dat");
        wi.data.assign(content.begin(), content.end());
        EnqueueWork(std::move(wi));
    }
}

std::unordered_map<std::string, std::string> LoadFileTokens(uint32_t accountId, uint32_t appId) {
    return LocalStorage::LoadFileTokens(accountId, appId);
}


bool SyncFromCloud(uint32_t accountId, uint32_t appId) {
    if (!g_provider || !g_provider->IsAuthenticated()) return false;

    auto syncStart = std::chrono::steady_clock::now();
    bool hadNewer = false;
    bool cloudHadNewerCN = false;
    bool cloudCNFound = false;
    bool cloudRootTokensFound = false;
    bool cloudFileTokensFound = false;
    std::unordered_set<std::string> cloudFileTokenNames;
    uint64_t localCN = 0;
    uint64_t cloudCN = 0;
    std::string storagePath = LocalStoragePath(accountId, appId);
    {
        std::error_code ec;
        std::filesystem::create_directories(storagePath, ec);
        if (ec) {
            LOG("[CloudStorage] SyncFromCloud app %u: failed to create local storage "
                "dir %s: %s — aborting sync", appId, storagePath.c_str(), ec.message().c_str());
            return false;
        }
    }
    std::unordered_set<std::string> originalRootTokens;
    std::unordered_map<std::string, std::string> originalFileTokens;
    std::unordered_set<std::string> mergedCloudRootTokens;
    std::unordered_map<std::string, std::string> mergedCloudFileTokens;
    bool haveOriginalTokenMetadata = false;
    bool haveMergedCloudRootTokens = false;
    bool haveMergedCloudFileTokens = false;
    bool rolledBackNewerCloudState = false;

    // 1. Sync CN: take max of local and cloud
    //    Read from LocalStorage's in-memory cache (matches Steam behavior where
    //    CN is read from an in-memory structure, not from disk).
    {
        localCN = LocalStorage::GetChangeNumber(accountId, appId);

        std::string cloudCNPath = CloudMetadataPath(accountId, appId, "cn.dat");
        std::vector<uint8_t> cloudData;
        if (g_provider->Download(cloudCNPath, cloudData)) {
            cloudCNFound = true;
            std::string s(cloudData.begin(), cloudData.end());
            try { cloudCN = std::stoull(s); } catch (...) {}
        }

        if (cloudCN > localCN) {
            LOG("[CloudStorage] SyncFromCloud app %u: cloud CN=%llu > local CN=%llu, using cloud",
                appId, cloudCN, localCN);
            LocalStorage::SetChangeNumber(accountId, appId, cloudCN);
            hadNewer = true;
            cloudHadNewerCN = true;
        } else if (localCN > cloudCN) {
            LOG("[CloudStorage] SyncFromCloud app %u: local CN=%llu > cloud CN=%llu, leaving provider unchanged until Steam uploads",
                appId, localCN, cloudCN);
        } else {
            LOG("[CloudStorage] SyncFromCloud app %u: CN in sync (local=%llu, cloud=%llu)",
                appId, localCN, cloudCN);
        }
    }

    // Snapshot unconditionally: cloudHadNewerCN may flip during processing, and
    // rollbackNewerCloudState must always be able to restore the pre-merge state.
    originalRootTokens = LocalStorage::LoadRootTokens(accountId, appId);
    originalFileTokens = LocalStorage::LoadFileTokens(accountId, appId);
    haveOriginalTokenMetadata = true;

    auto rollbackNewerCloudState = [&](const char* reason) {
        uint64_t currentCN = LocalStorage::GetChangeNumber(accountId, appId);
        if (currentCN == cloudCN) {
            LocalStorage::SetChangeNumber(accountId, appId, localCN);
        } else {
            LOG("[CloudStorage] SyncFromCloud app %u: preserving local CN=%llu during rollback; expected cloud CN=%llu",
                appId, currentCN, cloudCN);
        }
        if (haveOriginalTokenMetadata) {
            if (haveMergedCloudRootTokens && LocalStorage::LoadRootTokens(accountId, appId) == mergedCloudRootTokens) {
                LocalStorage::SaveRootTokens(accountId, appId, originalRootTokens);
            }
            if (haveMergedCloudFileTokens && LocalStorage::LoadFileTokens(accountId, appId) == mergedCloudFileTokens) {
                LocalStorage::SaveFileTokens(accountId, appId, originalFileTokens);
            }
        }
        cloudHadNewerCN = false;
        hadNewer = false;
        rolledBackNewerCloudState = true;
        LOG("[CloudStorage] SyncFromCloud app %u: rolled back newer cloud state because %s",
            appId, reason);
    };

    // 2. Sync root_token.dat: merge cloud tokens into local set
    {
        std::string cloudTokenPath = CloudMetadataPath(accountId, appId, "root_token.dat");
        std::vector<uint8_t> cloudData;
        if (g_provider->Download(cloudTokenPath, cloudData)) {
            cloudRootTokensFound = true;
            std::unordered_set<std::string> cloudTokens;
            bool cloudHadCorruption = false;
            std::istringstream iss(std::string(cloudData.begin(), cloudData.end()));
            std::string line;
            while (std::getline(iss, line)) {
                // Strip trailing \r / \n (CRLF corruption from Windows line endings)
                while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
                    line.pop_back();
                if (!line.empty()) cloudTokens.insert(line);
            }

            // Detect if cloud copy had more raw lines than cleaned tokens
            // (i.e., corrupted duplicates like "%Token%" and "%Token%\r")
            {
                size_t rawCount = 0;
                std::istringstream iss2(std::string(cloudData.begin(), cloudData.end()));
                std::string rawLine;
                while (std::getline(iss2, rawLine)) {
                    if (!rawLine.empty()) rawCount++;
                }
                if (rawCount > cloudTokens.size()) {
                    cloudHadCorruption = true;
                    LOG("[CloudStorage] SyncFromCloud app %u: cloud root_token.dat had %zu raw entries but only %zu clean tokens — pushing cleaned version",
                        appId, rawCount, cloudTokens.size());
                }
            }

            auto localTokens = LocalStorage::LoadRootTokens(accountId, appId);
            size_t beforeSize = localTokens.size();
            localTokens.insert(cloudTokens.begin(), cloudTokens.end());

            if (localTokens.size() > beforeSize) {
                LOG("[CloudStorage] SyncFromCloud app %u: merged %zu new root tokens from cloud",
                    appId, localTokens.size() - beforeSize);
                LocalStorage::SaveRootTokens(accountId, appId, localTokens);
                mergedCloudRootTokens = localTokens;
                haveMergedCloudRootTokens = true;
                hadNewer = true;
            }

            // If cloud had corrupted tokens, push the cleaned *cloud* set back --
            // NOT the local-merged superset. This is purely a serialization repair:
            // cloud already considered these tokens authoritative under its CN, we
            // are only fixing CRLF-duplicated entries. Uploading `localTokens` here
            // would leak local-only tokens to cloud that a later SyncFromCloud
            // rollback would revert locally, stranding them in cloud and
            // propagating them to other clients on the next sync.
            if (cloudHadCorruption) {
                std::string cleaned;
                for (auto& t : cloudTokens) {
                    cleaned += t + "\n";
                }
                std::vector<uint8_t> cleanedData(cleaned.begin(), cleaned.end());
                if (g_provider->Upload(cloudTokenPath, cleanedData.data(), cleanedData.size())) {
                    LOG("[CloudStorage] SyncFromCloud app %u: pushed cleaned root_token.dat to cloud (%zu tokens)",
                        appId, cloudTokens.size());
                } else {
                    LOG("[CloudStorage] SyncFromCloud app %u: FAILED to push cleaned root_token.dat to cloud",
                        appId);
                }
            }
        }
    }

    // 2b. Sync file_tokens.dat: merge cloud file-token mappings into local
    {
        std::string cloudPath = CloudMetadataPath(accountId, appId, "file_tokens.dat");
        std::vector<uint8_t> cloudData;
        if (g_provider->Download(cloudPath, cloudData)) {
            cloudFileTokensFound = true;
            if (!cloudData.empty()) {
                // Parse cloud file_tokens.dat
                std::unordered_map<std::string, std::string> cloudFileTokens;
                std::istringstream iss(std::string(cloudData.begin(), cloudData.end()));
                std::string line;
                while (std::getline(iss, line)) {
                    while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
                        line.pop_back();
                    if (line.empty()) continue;
                    auto tab = line.find('\t');
                    if (tab == std::string::npos) continue;
                    std::string cleanName = line.substr(0, tab);
                    std::string token = line.substr(tab + 1);
                    if (!cleanName.empty())
                        cloudFileTokens[cleanName] = token;
                }

                // Merge: cloud entries fill in any gaps in local
                auto localFileTokens = LocalStorage::LoadFileTokens(accountId, appId);
                size_t beforeSize = localFileTokens.size();
                bool changed = false;
                for (auto& [name, token] : cloudFileTokens) {
                    cloudFileTokenNames.insert(name);
                    auto localIt = localFileTokens.find(name);
                    if (localIt == localFileTokens.end() ||
                        (cloudHadNewerCN && localIt->second != token)) {
                        localFileTokens[name] = token;
                        changed = true;
                    }
                }
                if (changed) {
                    LOG("[CloudStorage] SyncFromCloud app %u: merged/updated file-token mappings from cloud (local %zu -> %zu)",
                        appId, beforeSize, localFileTokens.size());
                    LocalStorage::SaveFileTokens(accountId, appId, localFileTokens);
                    mergedCloudFileTokens = localFileTokens;
                    haveMergedCloudFileTokens = true;
                    hadNewer = true;
                }
            }
        }
    }

    // 3. (removed) metadata.json sync — no longer needed.
    //    Downloaded blobs are now written directly to LocalStorage,
    //    so GetFileList() works without a separate metadata file.

    // 4. Pre-populate blob cache: list cloud blobs, download any we don't have locally.
    //    Bounded by BLOB_SYNC_TIMEOUT_SEC to prevent blocking game launch indefinitely.
    //    Launch-time sync is download-only. Uploads are driven by Steam's
    //    BeginAppUploadBatch/CommitFileUpload flow after it classifies local
    //    files as changed, matching Steam's conflict-aware sync model.
    constexpr int BLOB_SYNC_TIMEOUT_SEC = 120; // 2 minutes max for blob downloads
    std::string blobPrefix = std::to_string(accountId) + "/" +
                             std::to_string(appId) + "/blobs/";
    std::vector<ICloudProvider::FileInfo> cloudBlobs;
    // cloudListComplete distinguishes a success-but-truncated listing (e.g.
    // GDrive hit MAX_RECURSION_DEPTH) from a verified-complete listing.
    // Prune decisions below refuse to run on incomplete listings to avoid
    // deleting local blobs that exist in cloud but weren't observed.
    bool cloudListComplete = false;
    bool cloudListSucceeded = g_provider->ListChecked(blobPrefix, cloudBlobs, &cloudListComplete);
    if (!cloudListSucceeded) {
        if (cloudHadNewerCN) {
            rollbackNewerCloudState("blob listing failed");
        }
        LOG("[CloudStorage] SyncFromCloud app %u: provider blob listing failed; skipping blob download/prune/recovery",
            appId);
        cloudBlobs.clear();
        cloudListComplete = false;
    } else if (!cloudListComplete) {
        LOG("[CloudStorage] SyncFromCloud app %u: provider blob listing returned partial results "
            "(e.g. recursion cap); downloads proceed but prune/gap-repair are skipped",
            appId);
    }
    std::unordered_set<std::string> cloudBlobNames;
    for (auto& fi : cloudBlobs) {
        auto blobsPos = fi.path.find("/blobs/");
        if (blobsPos == std::string::npos) continue;
        cloudBlobNames.insert(CanonicalizeInternalMetadataName(fi.path.substr(blobsPos + 7)));
    }

    // Load tombstones once per sync. A cloud Delete whose worker never
    // succeeded (provider down, permanent transport failure, etc.) would
    // otherwise resurrect the file here on the next SyncFromCloud. We do
    // NOT re-enqueue a Delete from this path — doing so would race a
    // concurrent StoreBlob+Upload via the FIFO queue and could lose the
    // user's freshly saved file. The tombstone stays until either the
    // original queued Delete finally succeeds or the user re-saves the
    // same filename (StoreBlob clears it).
    //
    // Each tombstone carries the local CN it was stamped with. If the
    // provider's CN is strictly greater, a different machine modified this
    // file after our local delete, and Steam's "newer CN wins" semantics
    // override the tombstone: we clear it and download normally. Otherwise
    // the tombstone stands.
    std::unordered_map<std::string, uint64_t> deletedTombstones =
        LocalStorage::LoadDeleted(accountId, appId);

    {
        struct StagedBlob {
            std::string filename;
            std::vector<uint8_t> data;
        };
        std::vector<StagedBlob> stagedNewerBlobs;
        int downloaded = 0, skipped = 0, failed = 0;
        bool timedOut = false;
        auto blobStart = std::chrono::steady_clock::now();
        for (auto& fi : cloudBlobs) {
            // Check timeout
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - blobStart).count();
            if (elapsed >= BLOB_SYNC_TIMEOUT_SEC) {
                int remaining = (int)cloudBlobs.size() - downloaded - skipped;
                LOG("[CloudStorage] SyncFromCloud app %u: blob download TIMEOUT after %llds, "
                    "%d downloaded, %d skipped, ~%d remaining",
                    appId, (long long)elapsed, downloaded, skipped, remaining);
                timedOut = true;
                break;
            }

            // fi.path is relative to provider root, e.g. "54303850/1229490/blobs/save.dat"
            // Extract filename from after "blobs/"
            auto blobsPos = fi.path.find("/blobs/");
            if (blobsPos == std::string::npos) continue;
            std::string filename = CanonicalizeInternalMetadataName(fi.path.substr(blobsPos + 7));

            // Honor local delete tombstones — don't resurrect files the user
            // deleted locally just because a not-yet-processed cloud Delete
            // hasn't flushed through the provider yet. Exception: if the
            // cloud CN is strictly newer than the tombstone CN, a different
            // machine wrote to this file after our delete and cloud wins.
            auto tombIt = deletedTombstones.find(filename);
            if (tombIt != deletedTombstones.end()) {
                if (cloudCNFound && cloudCN > tombIt->second) {
                    LOG("[CloudStorage] SyncFromCloud app %u: tombstone for %s "
                        "overridden by newer cloud CN (%llu > %llu) — clearing and downloading",
                        appId, filename.c_str(),
                        (unsigned long long)cloudCN,
                        (unsigned long long)tombIt->second);
                    LocalStorage::ClearDeleted(accountId, appId, filename);
                    deletedTombstones.erase(tombIt);
                    // fall through to normal download path
                } else {
                    skipped++;
                    LOG("[CloudStorage] SyncFromCloud app %u: skipping tombstoned blob %s "
                        "(tombstoneCn=%llu, cloudCn=%llu)",
                        appId, filename.c_str(),
                        (unsigned long long)tombIt->second,
                        (unsigned long long)cloudCN);
                    continue;
                }
            }

            std::string localBlobFile = LocalBlobPath(accountId, appId, filename);
            std::error_code existsEc;
            bool localExists = std::filesystem::exists(localBlobFile, existsEc);
            if (existsEc) localExists = false;
            if (localExists && !cloudHadNewerCN) {
                skipped++;
                continue; // already cached
            }

            // Download to local cache (atomic write)
            LOG("[CloudStorage] SyncFromCloud app %u: downloading blob %s...", appId, filename.c_str());
            std::vector<uint8_t> data;
            if (g_provider->Download(fi.path, data)) {
                if (cloudHadNewerCN) {
                    stagedNewerBlobs.push_back({ filename, std::move(data) });
                    downloaded++;
                    continue;
                }

                // Single authoritative write under LocalStorage::g_mutex so this
                // cannot race StoreBlob or RestoreFileIfUnchanged. CN was already
                // set from the cloud response in step 1 (matching real Steam
                // behavior where CN is set once, not per-file), hence No-Increment.
                const uint8_t* writeData = data.empty() ? nullptr : data.data();
                if (LocalStorage::WriteFileNoIncrement(accountId, appId, filename,
                                                       writeData, data.size())) {
                    downloaded++;
                    LOG("[CloudStorage] SyncFromCloud app %u: blob %s downloaded (%zu bytes)",
                        appId, filename.c_str(), data.size());
                } else {
                    failed++;
                    LOG("[CloudStorage] SyncFromCloud app %u: failed to write blob %s",
                        appId, filename.c_str());
                    continue;
                }
            } else {
                failed++;
                LOG("[CloudStorage] SyncFromCloud app %u: FAILED to download blob %s",
                    appId, filename.c_str());
            }
        }

        if (cloudHadNewerCN && failed == 0 && !timedOut) {
            struct PromotedBlob {
                std::string filename;
                std::string backupPath;
                std::vector<uint8_t> promotedData;
                bool hadOriginal = false;
            };
            std::vector<PromotedBlob> promoted;
            for (auto& staged : stagedNewerBlobs) {
                std::string localBlobFile = LocalBlobPath(accountId, appId, staged.filename);
                // Promotion is destructive (overwrites local). If we cannot
                // determine whether a local file exists (transient stat error
                // from AV lock, share violation, ACL glitch, etc.), we must
                // NOT treat that as "no local file" — that would skip the
                // conflict-copy safety net and let us clobber an unsynced
                // local save without a recoverable backup, and would also
                // leave hadOriginal=false so a later rollback would not
                // restore the original. Fail this staged blob into the
                // rollback path instead.
                std::error_code promoteEc;
                bool localExists = std::filesystem::exists(localBlobFile, promoteEc);
                if (promoteEc) {
                    LOG("[CloudStorage] SyncFromCloud app %u: stat failed for %s during "
                        "promotion (%s); failing batch to preserve local state",
                        appId, staged.filename.c_str(), promoteEc.message().c_str());
                    failed++;
                    break;
                }
                std::string backupPath;
                if (localExists) {
                    backupPath = CreateLocalConflictCopy(accountId, appId, staged.filename, localBlobFile);
                    if (backupPath.empty()) {
                        failed++;
                        break;
                    }
                }

                // Promote the staged blob under LocalStorage::g_mutex so this
                // write, StoreBlob, and the rollback RestoreFileIfUnchanged below
                // are all mutually exclusive. Without a shared mutex the atomic
                // filesystem rename can still clobber a concurrent StoreBlob and
                // RestoreFileIfUnchanged's compare-and-restore would miss it.
                const uint8_t* writeData = staged.data.empty() ? nullptr : staged.data.data();
                if (!LocalStorage::WriteFileNoIncrement(accountId, appId, staged.filename,
                                                       writeData, staged.data.size())) {
                    failed++;
                    LOG("[CloudStorage] SyncFromCloud app %u: failed to promote staged blob %s",
                        appId, staged.filename.c_str());
                    break;
                }
                promoted.push_back({ staged.filename, backupPath, staged.data, localExists });
                LOG("[CloudStorage] SyncFromCloud app %u: blob %s downloaded (%zu bytes)",
                    appId, staged.filename.c_str(), staged.data.size());
            }
            if (failed > 0) {
                // Rollback in reverse: holds LocalStorage's g_mutex so the
                // compare-and-restore is atomic vs. concurrent writers.
                for (auto it = promoted.rbegin(); it != promoted.rend(); ++it) {
                    LocalStorage::RestoreFileIfUnchanged(accountId, appId,
                                                        it->filename,
                                                        it->promotedData,
                                                        it->backupPath,
                                                        it->hadOriginal);
                }
            }
        }

        if (cloudHadNewerCN && (failed > 0 || timedOut)) {
            rollbackNewerCloudState("blob sync was incomplete");
        }
        if (downloaded > 0 && !rolledBackNewerCloudState) {
            LOG("[CloudStorage] SyncFromCloud app %u: downloaded %d blobs from cloud (skipped %d cached)",
                appId, downloaded, skipped);
            hadNewer = true;
        }
        // Prune guard: require a verified-complete listing. A success return
        // with cloudListComplete == false (e.g. GDrive recursion cap) must not
        // trigger RemoveLocalBlobsNotInCloud — files below the cap would be
        // silently deleted locally despite still existing in cloud.
        if (cloudHadNewerCN && cloudListSucceeded && cloudListComplete && !cloudBlobNames.empty()) {
            RemoveLocalBlobsNotInCloud(accountId, appId, cloudBlobNames);
        } else if (cloudHadNewerCN && cloudListSucceeded && cloudListComplete && cloudBlobNames.empty()) {
            LOG("[CloudStorage] SyncFromCloud app %u: empty blob listing is not explicit enough to prune local blobs",
                appId);
        } else if (cloudHadNewerCN && cloudListSucceeded && !cloudListComplete) {
            LOG("[CloudStorage] SyncFromCloud app %u: skipping local-blob prune because provider listing was incomplete",
                appId);
        }
    }

    // Recovery-only repair: when local is at least as current as the provider,
    // fill provider gaps from the local CloudRedirect cache. This preserves
    // existing users who link an empty provider or hit a partial provider upload
    // failure, while never overwriting a blob that already exists in cloud.
    // Gap-repair is a recovery path that trusts "cloud shows nothing" as
    // evidence of an uninitialized provider. A truncated listing (incomplete)
    // can look identically empty for a sub-tree while real data exists above
    // the recursion cap — never run recovery on an incomplete listing.
    bool providerLooksUninitialized = cloudListSucceeded && cloudListComplete &&
                                      !cloudCNFound && !cloudRootTokensFound &&
                                      !cloudFileTokensFound && cloudBlobNames.empty();
    bool canRepairProviderGaps = cloudListSucceeded && cloudListComplete &&
                                 localCN > 0 && providerLooksUninitialized;
    if (canRepairProviderGaps) {
        std::string localBlobDir = g_localRoot + "storage\\" +
                                   std::to_string(accountId) + "\\" +
                                   std::to_string(appId) + "\\";
        int seeded = 0;
        std::error_code gapEc;
        bool dirExists = std::filesystem::exists(localBlobDir, gapEc);
        if (gapEc) {
            LOG("[CloudStorage] SyncFromCloud app %u: gap-repair skipped — stat failed for %s: %s",
                appId, localBlobDir.c_str(), gapEc.message().c_str());
            dirExists = false;
        }
        if (dirExists) {
            std::error_code iterEc;
            std::filesystem::recursive_directory_iterator it(localBlobDir, iterEc);
            std::filesystem::recursive_directory_iterator end;
            if (iterEc) {
                LOG("[CloudStorage] SyncFromCloud app %u: gap-repair skipped — cannot open %s: %s",
                    appId, localBlobDir.c_str(), iterEc.message().c_str());
            } else {
                for (; !iterEc && it != end; it.increment(iterEc)) {
                    std::error_code entryEc;
                    const auto& entry = *it;
                    bool isFile = entry.is_regular_file(entryEc);
                    if (entryEc || !isFile) continue;

                    std::string rel = std::filesystem::relative(entry.path(), localBlobDir, entryEc).string();
                    if (entryEc) continue;
                    for (auto& c : rel) { if (c == '\\') c = '/'; }
                    if (rel == "cn.dat" || rel == "root_token.dat" ||
                        rel == "file_tokens.dat" || rel == "deleted.dat") continue;
                    if (cloudBlobNames.count(rel)) continue;

                    std::ifstream f(entry.path(), std::ios::binary);
                    if (!f) continue;
                    std::vector<uint8_t> data((std::istreambuf_iterator<char>(f)),
                                              std::istreambuf_iterator<char>());
                    WorkItem wi;
                    wi.type = WorkItem::Upload;
                    wi.cloudPath = CloudBlobPath(accountId, appId, rel);
                    wi.data = std::move(data);
                    wi.skipIfExists = true;
                    EnqueueWork(std::move(wi));
                    seeded++;
                }
                if (iterEc) {
                    LOG("[CloudStorage] SyncFromCloud app %u: gap-repair iteration aborted after %d seeded: %s",
                        appId, seeded, iterEc.message().c_str());
                }
            }
        }

        auto seedMeta = [&](const std::string& filename) {
            std::string localFile = storagePath + filename;
            std::error_code metaEc;
            if (!std::filesystem::exists(localFile, metaEc) || metaEc) return;

            std::ifstream f(localFile, std::ios::binary);
            if (!f) return;
            std::vector<uint8_t> data((std::istreambuf_iterator<char>(f)),
                                      std::istreambuf_iterator<char>());

            WorkItem wi;
            wi.type = WorkItem::Upload;
            wi.cloudPath = CloudMetadataPath(accountId, appId, filename);
            wi.data = std::move(data);
            wi.skipIfExists = true;
            EnqueueWork(std::move(wi));
            seeded++;
        };

        if (!cloudRootTokensFound) seedMeta("root_token.dat");
        if (!cloudFileTokensFound) seedMeta("file_tokens.dat");
        if (!cloudCNFound) seedMeta("cn.dat");

        if (seeded > 0) {
            LOG("[CloudStorage] SyncFromCloud app %u: recovered %d missing local cache file(s) to provider (%s)",
                appId, seeded, g_provider->Name());
        }
    }

    auto totalMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - syncStart).count();
    LOG("[CloudStorage] SyncFromCloud app %u: completed in %lld ms (hadNewer=%d)",
        appId, (long long)totalMs, hadNewer);

    return hadNewer;
}

std::vector<uint32_t> SyncAllFromCloud(uint32_t accountId) {
    std::vector<uint32_t> syncedApps;
    if (!g_provider || !g_provider->IsAuthenticated()) return syncedApps;

    LOG("[CloudStorage] SyncAllFromCloud: scanning for apps belonging to account %u...", accountId);

    // List all items under the account prefix to discover apps
    std::string prefix = std::to_string(accountId) + "/";
    auto items = g_provider->List(prefix);

    // Extract unique app IDs from paths like "54303850/1229490/cn.dat"
    std::unordered_set<uint32_t> appIds;
    for (auto& fi : items) {
        // path: "{accountId}/{appId}/..."
        auto firstSlash = fi.path.find('/');
        if (firstSlash == std::string::npos) continue;
        auto secondSlash = fi.path.find('/', firstSlash + 1);
        if (secondSlash == std::string::npos) continue;
        std::string appStr = fi.path.substr(firstSlash + 1, secondSlash - firstSlash - 1);
        try {
            appIds.insert(std::stoul(appStr));
        } catch (...) {}
    }

    if (appIds.empty()) {
        auto localAppIds = EnumerateLocalAppIds(accountId);
        if (!localAppIds.empty()) {
            LOG("[CloudStorage] SyncAllFromCloud: provider returned 0 apps, falling back to %zu local app(s)",
                localAppIds.size());
            appIds.insert(localAppIds.begin(), localAppIds.end());
        }
    }

    LOG("[CloudStorage] SyncAllFromCloud: found %zu apps in cloud", appIds.size());
    for (uint32_t appId : appIds) {
        SyncFromCloud(accountId, appId);
        syncedApps.push_back(appId);
    }

    return syncedApps;
}

void DrainQueue() {
    if (!g_provider) return;

    LOG("[CloudStorage] DrainQueue: waiting for background work to complete...");

    constexpr int TIMEOUT_MS = 30000;   // 30 seconds max wait
    auto start = std::chrono::steady_clock::now();

    std::unique_lock<std::mutex> lock(g_queueMutex);
    bool completed = g_drainCV.wait_for(lock,
        std::chrono::milliseconds(TIMEOUT_MS),
        [] { return g_workQueue.empty() && g_activeWorkers.load() == 0; });

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    if (completed) {
        LOG("[CloudStorage] DrainQueue: done (%lld ms)", (long long)elapsed);
    } else {
        LOG("[CloudStorage] DrainQueue: TIMEOUT after %lld ms, %zu queued, %d active",
            (long long)elapsed, g_workQueue.size(), g_activeWorkers.load());
    }
}

bool DrainQueueForApp(uint32_t accountId, uint32_t appId) {
    if (!g_provider) return true;

    std::string prefix = std::to_string(accountId) + "/" + std::to_string(appId) + "/";
    LOG("[CloudStorage] DrainQueueForApp: waiting for %s", prefix.c_str());

    constexpr int TIMEOUT_MS = 30000;
    auto start = std::chrono::steady_clock::now();

    std::unique_lock<std::mutex> lock(g_queueMutex);
    RequeueFailedWorkForPrefixLocked(prefix);
    g_queueCV.notify_all();
    bool completed = g_drainCV.wait_for(lock,
        std::chrono::milliseconds(TIMEOUT_MS),
        [&prefix] { return !HasPendingWorkForPrefix(prefix); });
    bool failed = HasFailedWorkForPrefix(prefix);

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    if (completed && !failed) {
        LOG("[CloudStorage] DrainQueueForApp: done for %s (%lld ms)",
            prefix.c_str(), (long long)elapsed);
    } else if (failed) {
        LOG("[CloudStorage] DrainQueueForApp: failed work for %s after %lld ms",
            prefix.c_str(), (long long)elapsed);
    } else {
        LOG("[CloudStorage] DrainQueueForApp: TIMEOUT for %s after %lld ms",
            prefix.c_str(), (long long)elapsed);
    }
    return completed && !failed;
}


} // namespace CloudStorage

// Factory implementation (declared in cloud_provider.h)
std::unique_ptr<ICloudProvider> CreateCloudProvider(const std::string& name) {
    // case-insensitive compare
    std::string lower = name;
    for (auto& c : lower) c = (char)tolower((unsigned char)c);

    if (lower == "local" || lower == "folder") {
        return std::make_unique<LocalDiskProvider>();
    }
    if (lower == "gdrive") {
        return std::make_unique<GoogleDriveProvider>();
    }
    if (lower == "onedrive") {
        return std::make_unique<OneDriveProvider>();
    }
    LOG("[CloudStorage] CreateCloudProvider: unknown provider '%s'", name.c_str());
    return nullptr;
}
