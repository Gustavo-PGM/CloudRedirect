#include "legacy_metadata_cleanup.h"
#include "file_util.h"
#include "log.h"

#include <array>
#include <exception>
#include <filesystem>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>

namespace LegacyMetadataCleanup {

namespace fs = std::filesystem;

namespace {

// Legacy filenames we scrub. Kept in sync with cloud_intercept.h's
// kLegacy*MetadataPath constants via a compile-time string_view comparison in
// the unit test so a rename in one place can't silently drift from the other.
constexpr std::array<std::string_view, 2> kLegacyNames = {
    "Playtime.bin",
    "UserGameStats.bin",
};

// Canonical subdirectory where current DLL stores these files (inside our
// private blob cache, never inside Steam's remote dir).
constexpr std::string_view kCanonicalDir = ".cloudredirect";

// Safe remove_file that swallows "not found" but reports genuine failures via
// ec_out so the caller can count errors separately from no-op misses.
bool RemoveFileIfPresent(const fs::path& p, SweepStats& stats) {
    std::error_code ec;
    if (!fs::exists(p, ec)) return false;
    if (fs::remove(p, ec)) {
        stats.filesRemoved++;
        LOG("[LegacyCleanup] removed %s", p.string().c_str());
        return true;
    }
    if (ec) {
        stats.errors++;
        LOG("[LegacyCleanup] failed to remove %s: %s",
            p.string().c_str(), ec.message().c_str());
    }
    return false;
}

// Remove `dir` and its contents. Must reject reparse points (junctions /
// symlinks) because this sweep runs unattended at DLL init against a
// user-writable tree — following a junction at `.cloudredirect\` into
// anywhere the user can create (their Documents, their save folder on
// another drive, etc.) would wipe arbitrary files. If we find a reparse
// point where we expect a plain directory, unlink only the link itself and
// never descend.
bool RemoveDirIfPresent(const fs::path& dir, SweepStats& stats) {
    std::error_code ec;
    auto symStat = fs::symlink_status(dir, ec);
    if (ec || !fs::exists(symStat)) return false;
    if (fs::is_symlink(symStat)) {
        if (fs::remove(dir, ec)) {
            stats.dirsRemoved++;
            LOG("[LegacyCleanup] unlinked reparse point %s (did not follow)",
                dir.string().c_str());
            return true;
        }
        if (ec) {
            stats.errors++;
            LOG("[LegacyCleanup] failed to unlink reparse point %s: %s",
                dir.string().c_str(), ec.message().c_str());
        }
        return false;
    }
    if (!fs::is_directory(symStat)) {
        // Something stole the name -- not our problem, leave it alone.
        return false;
    }
    // `remove_all` returns the number of files AND directories removed. Use
    // that as the authoritative post-condition count; counting beforehand
    // races with any concurrent writer (Steam actively touches the parent
    // remote\ directory during login). Explicitly do NOT follow symlinks
    // encountered during the descent.
    std::uintmax_t removed = fs::remove_all(dir, ec);
    if (ec) {
        stats.errors++;
        LOG("[LegacyCleanup] failed to remove dir %s: %s",
            dir.string().c_str(), ec.message().c_str());
        return false;
    }
    if (removed > 0) {
        // Subtract the directory itself from the file count so the dirsRemoved
        // bucket isn't double-counted.
        stats.filesRemoved += static_cast<int>(removed - 1);
        stats.dirsRemoved++;
        LOG("[LegacyCleanup] removed dir %s (%llu file(s)/subdir(s) inside)",
            dir.string().c_str(),
            static_cast<unsigned long long>(removed - 1));
        return true;
    }
    return false;
}

// Enumerate numeric-name subdirectories (account or app IDs). Anything else
// is ignored so we don't wander into arbitrary sibling directories the user
// may have dropped next to `userdata\`. Uses manual `increment(ec)` instead
// of a range-for because the range-for over `directory_iterator` uses the
// throwing `operator++` even when the constructor was given an error_code;
// mid-iteration failures (subdir disappeared, share violation, permission
// change) would otherwise escape out of DLL init as an uncaught
// `std::filesystem::filesystem_error`, crashing the host process.
std::vector<fs::path> ListNumericSubdirs(const fs::path& parent) {
    std::vector<fs::path> out;
    std::error_code ec;
    if (!fs::is_directory(parent, ec)) return out;
    fs::directory_iterator it(parent, fs::directory_options::skip_permission_denied, ec);
    fs::directory_iterator end;
    if (ec) return out;
    while (it != end) {
        std::error_code entryEc;
        // symlink_status: reparse points at the account/app level are
        // unexpected and we refuse to recurse through them. A real numeric-
        // named directory symlink is exotic enough that ignoring it is the
        // safer default.
        auto st = it->symlink_status(entryEc);
        if (!entryEc && !fs::is_symlink(st) && fs::is_directory(st)) {
            const std::string name = it->path().filename().string();
            bool allDigits = !name.empty();
            for (char c : name) {
                if (c < '0' || c > '9') { allDigits = false; break; }
            }
            if (allDigits) out.push_back(it->path());
        }
        std::error_code stepEc;
        it.increment(stepEc);
        if (stepEc) break;
    }
    return out;
}

} // namespace

SweepStats PruneSteamUserdata(const std::string& steamPath) {
    SweepStats stats;
    if (steamPath.empty()) return stats;

    try {
        // fs::path(std::string) narrows via ACP on Windows; route UTF-8
        // through Utf8ToPath so installs under non-ASCII profile folders
        // (Cyrillic/CJK usernames, accented paths) enumerate correctly.
        fs::path userdata = FileUtil::Utf8ToPath(steamPath) / "userdata";
        std::error_code ec;
        if (!fs::is_directory(userdata, ec)) return stats;

        for (const auto& acctDir : ListNumericSubdirs(userdata)) {
            for (const auto& appDir : ListNumericSubdirs(acctDir)) {
                fs::path remoteDir = appDir / "remote";
                if (!fs::is_directory(remoteDir, ec)) continue;

                // Top-level legacy files directly inside remote\.
                for (auto name : kLegacyNames) {
                    RemoveFileIfPresent(remoteDir / std::string(name), stats);
                }

                // The entire `.cloudredirect\` subdir under remote\ is an
                // artifact of a mid-evolution DLL that targeted Steam's remote
                // dir by mistake. Current DLL writes canonical metadata into
                // our private blob cache (`cloud_redirect\storage\...`) so
                // nothing of value lives here. RemoveDirIfPresent refuses to
                // follow reparse points.
                RemoveDirIfPresent(remoteDir / std::string(kCanonicalDir), stats);
            }
        }
    } catch (const std::exception& ex) {
        stats.errors++;
        LOG("[LegacyCleanup] userdata sweep aborted on exception: %s", ex.what());
    } catch (...) {
        stats.errors++;
        LOG("[LegacyCleanup] userdata sweep aborted on unknown exception");
    }

    if (stats.filesRemoved > 0 || stats.dirsRemoved > 0 || stats.errors > 0) {
        LOG("[LegacyCleanup] userdata sweep: %d file(s) removed, %d dir(s) removed, %d error(s)",
            stats.filesRemoved, stats.dirsRemoved, stats.errors);
    }
    return stats;
}

SweepStats PruneLocalBlobCache(const std::string& localRoot) {
    SweepStats stats;
    if (localRoot.empty()) return stats;

    try {
        // Same ACP-narrowing rationale as PruneSteamUserdata above.
        fs::path storage = FileUtil::Utf8ToPath(localRoot) / "storage";
        std::error_code ec;
        if (!fs::is_directory(storage, ec)) return stats;

        for (const auto& acctDir : ListNumericSubdirs(storage)) {
            for (const auto& appDir : ListNumericSubdirs(acctDir)) {
                fs::path canonDir = appDir / std::string(kCanonicalDir);
                for (auto name : kLegacyNames) {
                    fs::path legacy = appDir / std::string(name);
                    fs::path canon = canonDir / std::string(name);

                    // Never delete the only copy. If canonical is missing the
                    // next SyncFromCloud or StoreBlob will produce it via the
                    // filename canonicalization path; we'll pick up the
                    // leftover next time.
                    // Use symlink_status so a reparse point at the legacy
                    // path is NOT followed into unrelated territory.
                    std::error_code legEc, canEc;
                    auto legSt = fs::symlink_status(legacy, legEc);
                    auto canSt = fs::symlink_status(canon, canEc);
                    if (legEc || fs::is_symlink(legSt) || !fs::is_regular_file(legSt)) continue;
                    if (canEc || fs::is_symlink(canSt) || !fs::is_regular_file(canSt)) continue;

                    RemoveFileIfPresent(legacy, stats);
                }
            }
        }
    } catch (const std::exception& ex) {
        stats.errors++;
        LOG("[LegacyCleanup] local cache sweep aborted on exception: %s", ex.what());
    } catch (...) {
        stats.errors++;
        LOG("[LegacyCleanup] local cache sweep aborted on unknown exception");
    }

    if (stats.filesRemoved > 0 || stats.errors > 0) {
        LOG("[LegacyCleanup] local cache sweep: %d file(s) removed, %d error(s)",
            stats.filesRemoved, stats.errors);
    }
    return stats;
}

std::vector<std::string> ClassifyLegacyCloudBlobsToDelete(
    const std::vector<std::string>& cloudBlobRawPaths) {
    std::vector<std::string> toDelete;
    if (cloudBlobRawPaths.empty()) return toDelete;

    // Index every path by its `{acct}/{app}` prefix so we can check for the
    // canonical sibling without O(n^2) scanning. The set stores the filename
    // portion after `/blobs/` for each app prefix.
    struct AppEntry {
        std::unordered_set<std::string> filenames; // raw, as seen in listing
    };
    std::unordered_map<std::string, AppEntry> byApp;

    auto splitPath = [](const std::string& raw,
                        std::string& prefixOut,
                        std::string& filenameOut) -> bool {
        static const std::string kBlobs = "/blobs/";
        auto pos = raw.find(kBlobs);
        if (pos == std::string::npos) return false;
        prefixOut = raw.substr(0, pos);       // "{acct}/{app}"
        filenameOut = raw.substr(pos + kBlobs.size()); // rest
        if (filenameOut.empty()) return false;
        return true;
    };

    // First pass: populate index.
    for (const auto& raw : cloudBlobRawPaths) {
        std::string prefix, filename;
        if (!splitPath(raw, prefix, filename)) continue;
        byApp[prefix].filenames.insert(filename);
    }

    // Second pass: for each raw path that is a top-level legacy name, check
    // whether `.cloudredirect/{same}` is in the same app's filename set. Only
    // then flag the raw path for deletion.
    for (const auto& raw : cloudBlobRawPaths) {
        std::string prefix, filename;
        if (!splitPath(raw, prefix, filename)) continue;

        bool isLegacy = false;
        for (auto leg : kLegacyNames) {
            if (filename == leg) { isLegacy = true; break; }
        }
        if (!isLegacy) continue;

        const auto& app = byApp[prefix];
        std::string canonicalKey = std::string(kCanonicalDir) + "/" + filename;
        if (app.filenames.count(canonicalKey) > 0) {
            toDelete.push_back(raw);
        }
    }

    return toDelete;
}

} // namespace LegacyMetadataCleanup
