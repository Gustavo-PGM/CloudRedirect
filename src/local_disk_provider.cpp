#include "local_disk_provider.h"
#include "file_util.h"
#include "log.h"
#include <filesystem>
#include <fstream>

bool LocalDiskProvider::Init(const std::string& rootPath) {
    m_root = rootPath;
    if (!m_root.empty() && m_root.back() != '\\' && m_root.back() != '/')
        m_root += '\\';
    std::error_code ec;
    std::filesystem::create_directories(m_root, ec);
    if (ec) {
        LOG("[LocalDiskProvider] Failed to create root %s: %s", m_root.c_str(), ec.message().c_str());
        return false;
    }
    LOG("[LocalDiskProvider] Initialized at: %s", m_root.c_str());
    return true;
}

std::string LocalDiskProvider::ToFullPath(const std::string& relPath) const {
    std::string full = m_root + relPath;
    // normalize separators to backslash for Windows
    for (auto& c : full) {
        if (c == '/') c = '\\';
    }
    // FileUtil::IsPathWithin internally canonicalizes both sides via
    // weakly_canonical, so this check already resolves symlinks/junctions
    // inside the root and catches both "../" traversal and symlink-escape
    // attacks in a single call — do not add a second post-canonical check,
    // it would only duplicate FS I/O.
    if (!FileUtil::IsPathWithin(m_root, full)) {
        LOG("[LocalDiskProvider] BLOCKED path traversal: %s (root=%s)",
            relPath.c_str(), m_root.c_str());
        return {};
    }
    // Return canonical path (callers expect normalized result)
    std::error_code ec;
    auto canonical = std::filesystem::weakly_canonical(full, ec);
    if (ec) return {};
    return canonical.string();
}

bool LocalDiskProvider::Upload(const std::string& path,
                               const uint8_t* data, size_t len) {
    std::string full = ToFullPath(path);
    if (full.empty()) return false;
    auto parent = std::filesystem::path(full).parent_path();
    std::error_code ec;
    std::filesystem::create_directories(parent, ec);
    if (ec) {
        LOG("[LocalDiskProvider] Failed to create dirs for %s: %s", full.c_str(), ec.message().c_str());
        return false;
    }
    if (!FileUtil::AtomicWriteBinary(full, data, len)) {
        LOG("[LocalDiskProvider] Upload: atomic write failed %s (%zu bytes)", full.c_str(), len);
        return false;
    }
    return true;
}

bool LocalDiskProvider::Download(const std::string& path,
                                 std::vector<uint8_t>& outData) {
    std::string full = ToFullPath(path);
    if (full.empty()) return false;
    std::ifstream f(full, std::ios::binary);
    if (!f) return false;
    outData.assign(std::istreambuf_iterator<char>(f),
                   std::istreambuf_iterator<char>());
    return true;
}

bool LocalDiskProvider::Remove(const std::string& path) {
    std::string full = ToFullPath(path);
    if (full.empty()) return false;
    std::error_code ec;
    std::filesystem::remove(full, ec);
    // success if removed or never existed
    return !ec;
}

bool LocalDiskProvider::Exists(const std::string& path) {
    return CheckExists(path) == ExistsStatus::Exists;
}

ICloudProvider::ExistsStatus LocalDiskProvider::CheckExists(const std::string& path) {
    std::string full = ToFullPath(path);
    if (full.empty()) return ExistsStatus::Error;
    std::error_code ec;
    bool exists = std::filesystem::exists(full, ec);
    if (ec) return ExistsStatus::Error;
    if (!exists) return ExistsStatus::Missing;
    bool regular = std::filesystem::is_regular_file(full, ec);
    if (ec) return ExistsStatus::Error;
    return regular ? ExistsStatus::Exists : ExistsStatus::Missing;
}

std::vector<ICloudProvider::FileInfo> LocalDiskProvider::List(const std::string& prefix) {
    std::vector<FileInfo> result;
    ListChecked(prefix, result);
    return result;
}

bool LocalDiskProvider::ListChecked(const std::string& prefix, std::vector<FileInfo>& result,
                                    bool* outComplete) {
    result.clear();
    // Pessimistic default: any early-return path below except explicit success
    // must leave outComplete as false so the caller doesn't trust a partial
    // listing for destructive operations.
    if (outComplete) *outComplete = false;
    std::string dir = ToFullPath(prefix);
    if (dir.empty()) return false;
    std::error_code ec;
    bool exists = std::filesystem::exists(dir, ec);
    if (ec) return false;
    if (!exists) { if (outComplete) *outComplete = true; return true; }
    bool isDir = std::filesystem::is_directory(dir, ec);
    if (ec) return false;
    if (!isDir) { if (outComplete) *outComplete = true; return true; }

    // Capture both clocks once to avoid jitter from calling now() per file
    auto fileClockNow = std::filesystem::file_time_type::clock::now();
    auto sysClockNow = std::chrono::system_clock::now();

    // Do NOT use skip_permission_denied: it silently omits unreadable
    // subtrees from the listing while the iterator reports success. That
    // would let the caller (SyncFromCloud) treat the listing as a verified
    // inventory and delete local blobs the provider actually still holds.
    // A permission error is a listing failure for our purposes.
    std::filesystem::recursive_directory_iterator it(dir, ec);
    std::filesystem::recursive_directory_iterator end;
    // Per-entry filesystem errors (stale handle, concurrent deletion, AV
    // lock, reparse point race) must not silently drop files from the
    // listing. If any single stat fails, flag the listing as incomplete
    // so destructive prunes are suppressed; individual failures don't
    // collapse the whole sync to an error but they also don't pretend to
    // be authoritative.
    bool sawSkippedEntries = false;
    for (; !ec && it != end; it.increment(ec)) {
        const auto& entry = *it;
        std::error_code ec2;
        bool isFile = entry.is_regular_file(ec2);
        if (ec2) { sawSkippedEntries = true; continue; }
        if (!isFile) continue;

        std::string rel = std::filesystem::relative(entry.path(), m_root, ec2).string();
        if (ec2) { sawSkippedEntries = true; continue; }
        // normalize to forward slashes (ICloudProvider convention)
        for (auto& c : rel) {
            if (c == '\\') c = '/';
        }

        FileInfo fi;
        fi.path = rel;
        fi.size = entry.file_size(ec2);
        if (ec2) { sawSkippedEntries = true; continue; }

        auto ftime = std::filesystem::last_write_time(entry.path(), ec2);
        if (ec2) { sawSkippedEntries = true; continue; }
        auto sctp = std::chrono::time_point_cast<std::chrono::seconds>(
            ftime - fileClockNow + sysClockNow);
        fi.modifiedTime = (uint64_t)sctp.time_since_epoch().count();

        result.push_back(std::move(fi));
    }
    bool ok = !ec;
    if (ok && outComplete) *outComplete = !sawSkippedEntries;
    return ok;
}
