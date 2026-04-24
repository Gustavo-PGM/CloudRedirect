#include "legacy_metadata_cleanup.h"
#include "cloud_intercept.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace fs = std::filesystem;

static int g_fixtureCounter = 0;

static void Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << "\n";
        std::exit(1);
    }
}

static fs::path MakeTempDir(const std::string& tag) {
    fs::path base = fs::temp_directory_path() /
                    ("cr_legacy_cleanup_" + std::to_string(::GetCurrentProcessId()) +
                     "_" + std::to_string(++g_fixtureCounter) + "_" + tag);
    std::error_code ec;
    fs::remove_all(base, ec);
    fs::create_directories(base, ec);
    return base;
}

static void WriteFile(const fs::path& p, std::string_view content) {
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f.write(content.data(), static_cast<std::streamsize>(content.size()));
    f.close();
}

static bool Exists(const fs::path& p) {
    std::error_code ec;
    return fs::exists(p, ec);
}

// Ensures the string constants in legacy_metadata_cleanup.cpp stay bit-for-bit
// in sync with cloud_intercept.h. The sweep is useless (or worse, destructive
// of a legitimate file) if a rename in one place doesn't get picked up here.
static void TestConstantsLockstep() {
    // The sweep targets the two raw filenames that cloud_intercept.h declares
    // as legacy metadata. If somebody renames one without updating the other,
    // the classifier test below will also fail, but this check gives a
    // clearer error.
    Expect(std::string(CloudIntercept::kLegacyPlaytimeMetadataPath) == "Playtime.bin",
           "kLegacyPlaytimeMetadataPath drifted from Playtime.bin");
    Expect(std::string(CloudIntercept::kLegacyStatsMetadataPath) == "UserGameStats.bin",
           "kLegacyStatsMetadataPath drifted from UserGameStats.bin");
    Expect(std::string(CloudIntercept::kPlaytimeMetadataPath) == ".cloudredirect/Playtime.bin",
           "kPlaytimeMetadataPath drifted from .cloudredirect/Playtime.bin");
    Expect(std::string(CloudIntercept::kStatsMetadataPath) == ".cloudredirect/UserGameStats.bin",
           "kStatsMetadataPath drifted from .cloudredirect/UserGameStats.bin");
}

// Layer 1: Steam userdata sweep.
static void TestPruneSteamUserdata() {
    fs::path root = MakeTempDir("userdata");
    // Two accounts, three apps each, covering every combination we care about.
    //   acct1/appA : legacy top-level only                  -> removed
    //   acct1/appB : legacy + canonical inside .cloudredirect/ (wrong location) -> both gone + dir removed
    //   acct1/appC : only a real save file                  -> untouched
    //   acct2/appD : legacy + real .sav save                -> legacy gone, save preserved
    //   acct2/appE : only canonical dir inside remote       -> dir removed
    //   notAnAccount/ : non-numeric, must be ignored
    auto a1 = root / "userdata" / "54303850";
    auto a2 = root / "userdata" / "99999999";
    auto unrelated = root / "userdata" / "notAnAccount";
    fs::create_directories(unrelated);

    WriteFile(a1 / "1001" / "remote" / "Playtime.bin", "pt-a");
    WriteFile(a1 / "1001" / "remote" / "UserGameStats.bin", "ugs-a");

    WriteFile(a1 / "1002" / "remote" / "Playtime.bin", "pt-b");
    WriteFile(a1 / "1002" / "remote" / ".cloudredirect" / "Playtime.bin", "pt-b-canon");
    WriteFile(a1 / "1002" / "remote" / ".cloudredirect" / "UserGameStats.bin", "ugs-b-canon");

    WriteFile(a1 / "1003" / "remote" / "game.sav", "save-c");

    WriteFile(a2 / "2001" / "remote" / "Playtime.bin", "pt-d");
    WriteFile(a2 / "2001" / "remote" / "slot1.sav", "save-d");

    WriteFile(a2 / "2002" / "remote" / ".cloudredirect" / "Playtime.bin", "pt-e-canon");

    // Sibling non-userdata directory at Steam root must be left alone.
    WriteFile(root / "steamapps" / "common" / "Foo" / "Playtime.bin", "unrelated");

    // PruneSteamUserdata expects steamPath with trailing backslash (matches
    // cloud_intercept convention).
    std::string steamPath = root.string();
    if (!steamPath.empty() && steamPath.back() != '\\' && steamPath.back() != '/')
        steamPath += '\\';

    auto stats = LegacyMetadataCleanup::PruneSteamUserdata(steamPath);

    Expect(!Exists(a1 / "1001" / "remote" / "Playtime.bin"),
           "acct1/appA Playtime.bin should be removed");
    Expect(!Exists(a1 / "1001" / "remote" / "UserGameStats.bin"),
           "acct1/appA UserGameStats.bin should be removed");

    Expect(!Exists(a1 / "1002" / "remote" / "Playtime.bin"),
           "acct1/appB top-level Playtime.bin should be removed");
    Expect(!Exists(a1 / "1002" / "remote" / ".cloudredirect"),
           "acct1/appB .cloudredirect subdir should be removed entirely");

    Expect(Exists(a1 / "1003" / "remote" / "game.sav"),
           "acct1/appC unrelated save must be preserved");

    Expect(!Exists(a2 / "2001" / "remote" / "Playtime.bin"),
           "acct2/appD legacy Playtime.bin should be removed");
    Expect(Exists(a2 / "2001" / "remote" / "slot1.sav"),
           "acct2/appD real save must be preserved");

    Expect(!Exists(a2 / "2002" / "remote" / ".cloudredirect"),
           "acct2/appE .cloudredirect subdir should be removed");

    Expect(Exists(root / "steamapps" / "common" / "Foo" / "Playtime.bin"),
           "non-userdata Playtime.bin outside userdata must be untouched");

    // Stats: 4 top-level files (1001x2, 1002x1, 2001x1) + dir contents
    //  1002/.cloudredirect has 2 files, 2002/.cloudredirect has 1 file.
    // So filesRemoved >= 4 + 3 = 7, dirsRemoved = 2.
    Expect(stats.filesRemoved >= 7, "expected at least 7 files removed");
    Expect(stats.dirsRemoved == 2, "expected exactly 2 .cloudredirect dirs removed");
    Expect(stats.errors == 0, "sweep should not report errors on a clean fixture");

    // Idempotent: second call is a no-op.
    auto stats2 = LegacyMetadataCleanup::PruneSteamUserdata(steamPath);
    Expect(stats2.filesRemoved == 0 && stats2.dirsRemoved == 0 && stats2.errors == 0,
           "second sweep should be a no-op");

    std::error_code ec;
    fs::remove_all(root, ec);
}

// Missing `userdata\` subdirectory is a successful no-op.
static void TestPruneSteamUserdataMissingDir() {
    fs::path root = MakeTempDir("no_userdata");
    std::string steamPath = root.string();
    if (!steamPath.empty() && steamPath.back() != '\\' && steamPath.back() != '/')
        steamPath += '\\';
    auto stats = LegacyMetadataCleanup::PruneSteamUserdata(steamPath);
    Expect(stats.filesRemoved == 0 && stats.dirsRemoved == 0 && stats.errors == 0,
           "missing userdata dir should produce no work and no errors");
    std::error_code ec;
    fs::remove_all(root, ec);
}

// Layer 2: local blob cache dedup.
static void TestPruneLocalBlobCache() {
    fs::path root = MakeTempDir("local_cache");
    //   acct/appA : legacy + canonical present          -> legacy removed, canonical kept
    //   acct/appB : legacy only (no canonical)          -> legacy preserved (only copy)
    //   acct/appC : canonical only                      -> untouched
    //   acct/appD : unrelated top-level file            -> untouched
    //   nonNumeric/ : not descended into
    auto storage = root / "storage" / "54303850";
    auto nonNumeric = root / "storage" / "oddball";
    fs::create_directories(nonNumeric);
    WriteFile(nonNumeric / "Playtime.bin", "should-not-be-touched");

    WriteFile(storage / "1000" / "Playtime.bin", "legacy-pt");
    WriteFile(storage / "1000" / "UserGameStats.bin", "legacy-ugs");
    WriteFile(storage / "1000" / ".cloudredirect" / "Playtime.bin", "canon-pt");
    WriteFile(storage / "1000" / ".cloudredirect" / "UserGameStats.bin", "canon-ugs");

    WriteFile(storage / "1001" / "Playtime.bin", "only-copy");
    WriteFile(storage / "1001" / "UserGameStats.bin", "only-copy-ugs");

    WriteFile(storage / "1002" / ".cloudredirect" / "Playtime.bin", "canon-only");
    WriteFile(storage / "1002" / ".cloudredirect" / "UserGameStats.bin", "canon-only-ugs");

    WriteFile(storage / "1003" / "save.dat", "real-save");

    std::string localRoot = root.string();
    if (!localRoot.empty() && localRoot.back() != '\\' && localRoot.back() != '/')
        localRoot += '\\';

    auto stats = LegacyMetadataCleanup::PruneLocalBlobCache(localRoot);

    Expect(!Exists(storage / "1000" / "Playtime.bin"),
           "appA: legacy Playtime.bin should be removed (canonical present)");
    Expect(!Exists(storage / "1000" / "UserGameStats.bin"),
           "appA: legacy UserGameStats.bin should be removed (canonical present)");
    Expect(Exists(storage / "1000" / ".cloudredirect" / "Playtime.bin"),
           "appA: canonical Playtime.bin must be preserved");
    Expect(Exists(storage / "1000" / ".cloudredirect" / "UserGameStats.bin"),
           "appA: canonical UserGameStats.bin must be preserved");

    Expect(Exists(storage / "1001" / "Playtime.bin"),
           "appB: legacy Playtime.bin must be preserved (it's the only copy)");
    Expect(Exists(storage / "1001" / "UserGameStats.bin"),
           "appB: legacy UserGameStats.bin must be preserved (it's the only copy)");

    Expect(Exists(storage / "1002" / ".cloudredirect" / "Playtime.bin"),
           "appC: canonical Playtime.bin must be preserved");

    Expect(Exists(storage / "1003" / "save.dat"),
           "appD: unrelated file must be preserved");

    Expect(Exists(nonNumeric / "Playtime.bin"),
           "non-numeric sibling dirs must be ignored");

    Expect(stats.filesRemoved == 2, "expected exactly 2 legacy files removed");
    Expect(stats.errors == 0, "no errors on clean fixture");

    // Idempotent second run
    auto stats2 = LegacyMetadataCleanup::PruneLocalBlobCache(localRoot);
    Expect(stats2.filesRemoved == 0, "second sweep should be a no-op");

    std::error_code ec;
    fs::remove_all(root, ec);
}

// Layer 3: cloud classifier.
static void TestClassifyLegacyCloudBlobs() {
    // Legacy + canonical both present -> legacy returned.
    // Legacy only (no canonical) -> skipped.
    // Canonical only -> skipped.
    // Non-metadata files -> skipped.
    // Other app's canonical must NOT satisfy this app's legacy (per-prefix scoping).
    std::vector<std::string> listing = {
        // app 1000: legacy + canonical Playtime, legacy + canonical UserGameStats
        "54303850/1000/blobs/Playtime.bin",
        "54303850/1000/blobs/.cloudredirect/Playtime.bin",
        "54303850/1000/blobs/UserGameStats.bin",
        "54303850/1000/blobs/.cloudredirect/UserGameStats.bin",
        "54303850/1000/blobs/save.dat",

        // app 1001: legacy Playtime only, no canonical -> must NOT be returned
        "54303850/1001/blobs/Playtime.bin",

        // app 1002: canonical only (no legacy)
        "54303850/1002/blobs/.cloudredirect/Playtime.bin",

        // app 1003: legacy Playtime on THIS app, canonical in a DIFFERENT app -> must NOT be returned
        "54303850/1003/blobs/Playtime.bin",
        "54303850/9999/blobs/.cloudredirect/Playtime.bin",

        // unusual but valid: file without /blobs/ is ignored
        "malformed/path",
    };

    auto deletions = LegacyMetadataCleanup::ClassifyLegacyCloudBlobsToDelete(listing);

    auto contains = [&](const std::string& p) {
        return std::find(deletions.begin(), deletions.end(), p) != deletions.end();
    };
    Expect(contains("54303850/1000/blobs/Playtime.bin"),
           "app 1000 legacy Playtime should be queued for delete");
    Expect(contains("54303850/1000/blobs/UserGameStats.bin"),
           "app 1000 legacy UserGameStats should be queued for delete");
    Expect(!contains("54303850/1001/blobs/Playtime.bin"),
           "app 1001 legacy-only must NOT be queued (would destroy only copy)");
    Expect(!contains("54303850/1003/blobs/Playtime.bin"),
           "app 1003 legacy must NOT be queued on sibling-app canonical match");
    Expect(!contains("54303850/1000/blobs/.cloudredirect/Playtime.bin"),
           "canonical path must never be queued for delete");
    Expect(deletions.size() == 2, "exactly 2 legacy deletions expected");

    // Empty input is a no-op.
    auto none = LegacyMetadataCleanup::ClassifyLegacyCloudBlobsToDelete({});
    Expect(none.empty(), "empty listing should produce empty deletion set");
}

// Security hardening: `.cloudredirect\` inside remote\ must be unlinked if it
// is a directory reparse point (junction / symlink), never recursed into.
// Steam's userdata tree is user-writable; an attacker or careless user could
// repoint `.cloudredirect\` at arbitrary data and have the sweep wipe it.
static void TestPruneSteamUserdataRefusesSymlinkDescent() {
    fs::path root = MakeTempDir("userdata_symlink");
    fs::path victim = MakeTempDir("victim_target");
    WriteFile(victim / "important_user_file.txt", "user-data-must-survive");

    fs::path remoteDir = root / "userdata" / "54303850" / "1000" / "remote";
    fs::create_directories(remoteDir);

    // Create a directory junction at remoteDir\.cloudredirect pointing at victim.
    // `fs::create_directory_symlink` requires either admin rights or Windows
    // Developer Mode. If the privilege isn't available we skip the test with
    // a clear message so CI isn't silently passing on unprivileged runners.
    fs::path link = remoteDir / ".cloudredirect";
    std::error_code linkEc;
    fs::create_directory_symlink(victim, link, linkEc);
    if (linkEc) {
        std::cerr << "NOTE: TestPruneSteamUserdataRefusesSymlinkDescent skipped"
                  << " (could not create directory symlink: "
                  << linkEc.message()
                  << "). Requires Developer Mode or admin on Windows.\n";
        std::error_code cleanupEc;
        fs::remove_all(root, cleanupEc);
        fs::remove_all(victim, cleanupEc);
        return;
    }

    std::string steamPath = root.string();
    if (!steamPath.empty() && steamPath.back() != '\\' && steamPath.back() != '/')
        steamPath += '\\';

    auto stats = LegacyMetadataCleanup::PruneSteamUserdata(steamPath);

    // The symlink itself should be gone (unlinked).
    Expect(!Exists(link),
           "reparse point at remote\\.cloudredirect must be unlinked");
    // The victim target and its contents must survive — the sweep must have
    // unlinked the junction WITHOUT descending into the target.
    Expect(Exists(victim),
           "symlink target directory must survive the sweep");
    Expect(Exists(victim / "important_user_file.txt"),
           "files inside the symlink target must survive the sweep");
    Expect(stats.errors == 0,
           "reparse-point unlink should not be flagged as an error");

    std::error_code ec;
    fs::remove_all(root, ec);
    fs::remove_all(victim, ec);
}

int main() {
    TestConstantsLockstep();
    TestPruneSteamUserdataMissingDir();
    TestPruneSteamUserdata();
    TestPruneLocalBlobCache();
    TestClassifyLegacyCloudBlobs();
    TestPruneSteamUserdataRefusesSymlinkDescent();
    std::cout << "All legacy_metadata_cleanup_tests passed.\n";
    return 0;
}
