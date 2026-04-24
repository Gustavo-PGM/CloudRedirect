#pragma once
// Shared file utilities: atomic writes, path traversal validation.

#include <string>
#include <fstream>
#include <filesystem>
#include <cstdint>
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

namespace FileUtil {

// Path containment check: returns true if `fullPath` resolves to a location
// within `root` after canonicalization. Case-insensitive on Windows.
// Used to prevent path traversal attacks across blob storage modules.
inline bool IsPathWithin(const std::string& root, const std::string& fullPath) {
    std::error_code ec;
    auto canonRoot = std::filesystem::weakly_canonical(root, ec);
    if (ec) return false;
    auto canonPath = std::filesystem::weakly_canonical(fullPath, ec);
    if (ec) return false;
    std::string rootStr = canonRoot.string();
    std::string pathStr = canonPath.string();
    if (pathStr.size() < rootStr.size()) return false;
    if (_strnicmp(pathStr.c_str(), rootStr.c_str(), rootStr.size()) != 0) return false;
    // Exact match (path == root) or next char must be a separator
    return pathStr.size() == rootStr.size() ||
           pathStr[rootStr.size()] == '\\' ||
           pathStr[rootStr.size()] == '/';
}

// Walk up from `startDir` removing empty directories until the walk reaches
// `stopAt` (exclusive) or hits a non-empty / non-existent directory. Best-effort
// cleanup — every error path silently stops the walk. Used by the blob cache
// to prune session-scoped subtrees (e.g. Unity's Analytics/ArchivedEvents/<id>/)
// after files inside them are deleted.
//
// Safety:
//   - The walk is bounded by a containment check against `stopAt` on every
//     iteration; it cannot climb above that root even if the filesystem has
//     unusual parent-chain shapes.
//   - std::filesystem::remove() on a non-empty dir fails with ec set and
//     returns false, so the walk stops at the first dir that still has content.
//   - A 256-step hard cap protects against pathological filesystems (loops,
//     symlinks canonicalization glitches); real cache trees are <10 deep.
//   - Case-insensitive on Windows so NTFS case-folding doesn't break the
//     containment check after canonicalization.
inline void CleanupEmptyDirsUpTo(const std::string& startDir,
                                 const std::string& stopAt) {
    if (startDir.empty() || stopAt.empty()) return;
    std::error_code ec;
    auto canonStop = std::filesystem::weakly_canonical(stopAt, ec);
    if (ec) return;
    auto cur = std::filesystem::weakly_canonical(startDir, ec);
    if (ec) return;

    const std::string stopStr = canonStop.string();

    for (int i = 0; i < 256; ++i) {
        const std::string curStr = cur.string();
        // Must be strictly under stopAt — equal or outside stops the walk.
        if (curStr.size() <= stopStr.size()) break;
        if (_strnicmp(curStr.c_str(), stopStr.c_str(), stopStr.size()) != 0) break;
        const char sep = curStr[stopStr.size()];
        if (sep != '\\' && sep != '/') break;

        ec.clear();
        bool removed = std::filesystem::remove(cur, ec);
        // ec set   -> non-empty dir or permission/IO error; stop.
        // !removed -> dir did not exist; stop (parent walk would be spurious).
        if (ec || !removed) break;

        if (!cur.has_parent_path()) break;
        auto parent = cur.parent_path();
        if (parent == cur) break; // reached filesystem root
        cur = std::move(parent);
    }
}

inline bool AtomicWriteBinary(const std::string& path, const void* data, size_t len) {
    std::string tmpPath = path + ".tmp";
    std::ofstream f(tmpPath, std::ios::binary);
    if (!f) return false;
    if (len != 0) {
        f.write(static_cast<const char*>(data), len);
    }
    if (!f.good()) {
        f.close();
        std::error_code ec;
        std::filesystem::remove(tmpPath, ec);
        return false;
    }
    f.close();
    if (!MoveFileExA(tmpPath.c_str(), path.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        std::error_code ec;
        std::filesystem::remove(tmpPath, ec);
        return false;
    }
    return true;
}

inline bool AtomicWriteText(const std::string& path, const std::string& content) {
    std::string tmpPath = path + ".tmp";
    std::ofstream f(tmpPath, std::ios::trunc);
    if (!f) return false;
    f << content;
    if (!f.good()) {
        f.close();
        std::error_code ec;
        std::filesystem::remove(tmpPath, ec);
        return false;
    }
    f.close();
    if (!MoveFileExA(tmpPath.c_str(), path.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        std::error_code ec;
        std::filesystem::remove(tmpPath, ec);
        return false;
    }
    return true;
}

} // namespace FileUtil
