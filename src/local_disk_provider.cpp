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
    std::string full = ToFullPath(path);
    if (full.empty()) return false;
    return std::filesystem::exists(full) && std::filesystem::is_regular_file(full);
}

std::vector<ICloudProvider::FileInfo> LocalDiskProvider::List(const std::string& prefix) {
    std::vector<FileInfo> result;
    std::string dir = ToFullPath(prefix);
    if (dir.empty() || !std::filesystem::exists(dir) || !std::filesystem::is_directory(dir))
        return result;

    // Capture both clocks once to avoid jitter from calling now() per file
    auto fileClockNow = std::filesystem::file_time_type::clock::now();
    auto sysClockNow = std::chrono::system_clock::now();

    for (auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        std::string rel = std::filesystem::relative(entry.path(), m_root).string();
        // normalize to forward slashes (ICloudProvider convention)
        for (auto& c : rel) {
            if (c == '\\') c = '/';
        }

        FileInfo fi;
        fi.path = rel;
        fi.size = entry.file_size();

        auto ftime = std::filesystem::last_write_time(entry.path());
        auto sctp = std::chrono::time_point_cast<std::chrono::seconds>(
            ftime - fileClockNow + sysClockNow);
        fi.modifiedTime = (uint64_t)sctp.time_since_epoch().count();

        result.push_back(std::move(fi));
    }
    return result;
}
