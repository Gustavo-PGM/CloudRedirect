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

// Path-redirecting reparse-point detection: returns true ONLY for reparse
// tags that let an attacker redirect a recursive scan to a path of their
// choosing — NTFS junctions (IO_REPARSE_TAG_MOUNT_POINT) and symlinks
// (IO_REPARSE_TAG_SYMLINK). Returns false for benign reparse tags
// (OneDrive Files-on-Demand cloud placeholders, AppExecLink, WSL, WIM,
// dedup, etc.) so legitimate cloud-synced saves are not refused.
//
// Threat model: AutoCloud rules walk %USERPROFILE%\Documents\... and other
// locations a malicious save file or game patcher can write to. A junction
// dropped under the rule's resolved scan path would otherwise let the walk
// follow it into e.g. C:\Users\<u>\.aws\ and exfiltrate the contents into the
// cloud blob upload. Junctions don't need SeCreateSymbolicLinkPrivilege
// (mklink /J runs unprivileged), so this is a realistic primitive.
//
// Why explicit tag inspection instead of FILE_ATTRIBUTE_REPARSE_POINT alone:
//   - OneDrive Files-on-Demand placeholders set FILE_ATTRIBUTE_REPARSE_POINT
//     with IO_REPARSE_TAG_CLOUD_* — opening reads them through the cloud
//     filter and returns the real bytes. Refusing all reparse points would
//     silently break AutoCloud for every OneDrive user with synced saves.
//   - AppExecLink (Microsoft Store apps), WSL, dedup, WIM and similar tags
//     are not under attacker control in the AutoCloud scan paths and don't
//     redirect to attacker-chosen targets.
//
// Why is_symlink() doesn't work:
//   - MSVC STL classifies NTFS junctions as file_type::junction (a non-
//     standard extension), not file_type::symlink. is_symlink() returns
//     false for junctions. Filtering only via is_symlink misses the most
//     common attacker primitive on Windows.
//
// Why FindFirstFileW and not GetFileAttributesW:
//   - GetFileAttributesW reports the reparse attribute bit but not the tag.
//   - FindFirstFileW returns the tag in WIN32_FIND_DATAW::dwReserved0 for
//     reparse-point entries (documented contract), in a single syscall.
//
// Returns false on any error (path missing, ACL denied) — callers treat
// "unknown" as "not a reparse point" and let the subsequent open/walk fail
// naturally with its own error path. This avoids a denial-of-service where a
// transiently unreadable directory permanently blocks AutoCloud.
inline bool IsPathRedirectingReparsePoint(const std::string& path) {
    int wideLen = MultiByteToWideChar(CP_UTF8, 0, path.c_str(), -1, nullptr, 0);
    if (wideLen <= 0) return false;
    std::wstring wide(static_cast<size_t>(wideLen - 1), L'\0');
    if (MultiByteToWideChar(CP_UTF8, 0, path.c_str(), -1, wide.data(), wideLen) <= 0) {
        return false;
    }
    WIN32_FIND_DATAW fd{};
    HANDLE h = FindFirstFileW(wide.c_str(), &fd);
    if (h == INVALID_HANDLE_VALUE) return false;
    FindClose(h);
    if ((fd.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0) return false;
    // dwReserved0 carries the reparse tag for entries that are reparse points.
    switch (fd.dwReserved0) {
        case IO_REPARSE_TAG_MOUNT_POINT: // NTFS junction — unprivileged primitive
        case IO_REPARSE_TAG_SYMLINK:     // file/dir symlink — privileged primitive
            return true;
        default:
            // CLOUD_*, APPEXECLINK, WCI*, WIM, DEDUP, HSM, GVFS, ONEDRIVE, etc.
            return false;
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
