#include "local_storage.h"
#include "file_util.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

static void Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << message << "\n";
        std::exit(1);
    }
}

int main() {
    std::string root;
    std::string resolved;

    LocalStorage::TestResolveAutoCloudRootOverride(
        "LinuxXdgConfigHome", "UnderTheIsland", "LinuxXdgConfigHome",
        "WinAppDataLocal", "", "UnderTheIsland", "UnderTheIsland", root, resolved);
    Expect(root == "WinAppDataLocal", "useinstead should canonicalize the rule root");
    Expect(resolved == "UnderTheIsland", "path transform should preserve transformed scan path");

    LocalStorage::TestResolveAutoCloudRootOverride(
        "WinMyDocuments", "settings", "WinMyDocuments",
        "", "Battlefield 6", "", "", root, resolved);
    Expect(root == "WinMyDocuments", "addpath-only override should keep original root");
    Expect(resolved == "Battlefield 6/settings", "addpath-only override should prefix scan path");

    Expect(LocalStorage::TestIsSafeAutoCloudRelativePath("Saves/Slot1"), "relative paths should be accepted");
    Expect(!LocalStorage::TestIsSafeAutoCloudRelativePath("../Saves"), "parent traversal should be rejected");
    Expect(!LocalStorage::TestIsSafeAutoCloudRelativePath("C:/Users/Save"), "absolute drive paths should be rejected");
    Expect(LocalStorage::TestParseMinimalAutoCloudKVFixture(), "native appinfo KV parser should extract UFS savefile rules");
    Expect(LocalStorage::TestAutoCloudPlatformAndExcludeFilters(),
           "savefiles platforms/exclude filters should be honored");

    // Siblings splitter: baseline space-delimited parse.
    {
        auto s = LocalStorage::TestParseAutoCloudSiblings("meta thumb");
        Expect(s.size() == 2 && s[0] == "meta" && s[1] == "thumb",
               "siblings splitter should produce two tokens for 'meta thumb'");
    }
    // Siblings splitter: tabs and surrounding whitespace are tolerated.
    {
        auto s = LocalStorage::TestParseAutoCloudSiblings("  meta\tthumb\t\t ");
        Expect(s.size() == 2 && s[0] == "meta" && s[1] == "thumb",
               "siblings splitter should ignore leading/trailing whitespace and split on tab");
    }
    // Siblings splitter: empty / whitespace-only input yields empty vector.
    {
        auto s0 = LocalStorage::TestParseAutoCloudSiblings("");
        auto s1 = LocalStorage::TestParseAutoCloudSiblings("   \t \t");
        Expect(s0.empty(), "empty siblings string should yield empty vector");
        Expect(s1.empty(), "whitespace-only siblings string should yield empty vector");
    }
    // Siblings splitter: path-safety filter drops hostile tokens but
    // preserves legitimate neighbors. This is the save-safety line —
    // bad tokens must not cause the whole rule to be dropped, and
    // good tokens must not be collateral damage of the filter.
    {
        auto s = LocalStorage::TestParseAutoCloudSiblings(
            "meta ../evil .hidden /bad \\winbad .. thumb\x01 good");
        // Expected survivors: "meta", "thumb\x01" rejected (control char),
        // "good" kept. So surviving = ["meta", "good"].
        Expect(s.size() == 2 && s[0] == "meta" && s[1] == "good",
               "siblings splitter should reject unsafe tokens while keeping legitimate neighbors");
    }
    // Siblings splitter: leading-dot tokens are rejected (Steam prepends
    // its own dot when composing, so ".meta" would yield "stem..meta"
    // which is never a real file — rejecting matches effective outcome).
    {
        auto s = LocalStorage::TestParseAutoCloudSiblings(".meta meta");
        Expect(s.size() == 1 && s[0] == "meta",
               "siblings splitter should reject leading-dot tokens");
    }

    // CleanupEmptyDirsUpTo: removes empty dir chain but preserves stopAt root.
    {
        std::error_code ec;
        auto tmp = std::filesystem::temp_directory_path(ec) /
                   "cloudredirect_cleanup_test_1";
        std::filesystem::remove_all(tmp, ec);
        auto leaf = tmp / "a" / "b" / "c";
        std::filesystem::create_directories(leaf, ec);
        Expect(!ec, "test-1 setup: create_directories");

        FileUtil::CleanupEmptyDirsUpTo(leaf.string(), tmp.string());

        Expect(!std::filesystem::exists(tmp / "a", ec),
               "CleanupEmptyDirsUpTo should remove entire empty chain");
        Expect(std::filesystem::exists(tmp, ec),
               "CleanupEmptyDirsUpTo must preserve the stopAt root");
        std::filesystem::remove_all(tmp, ec);
    }

    // CleanupEmptyDirsUpTo: stops at first non-empty ancestor.
    {
        std::error_code ec;
        auto tmp = std::filesystem::temp_directory_path(ec) /
                   "cloudredirect_cleanup_test_2";
        std::filesystem::remove_all(tmp, ec);
        auto leaf = tmp / "a" / "b" / "c";
        std::filesystem::create_directories(leaf, ec);
        // Put a sibling file inside 'a' so it cannot be removed.
        std::ofstream(tmp / "a" / "keep.txt") << "x";

        FileUtil::CleanupEmptyDirsUpTo(leaf.string(), tmp.string());

        Expect(!std::filesystem::exists(tmp / "a" / "b", ec),
               "CleanupEmptyDirsUpTo should remove empty 'b' subtree");
        Expect(std::filesystem::exists(tmp / "a", ec),
               "CleanupEmptyDirsUpTo must not remove non-empty ancestor");
        Expect(std::filesystem::exists(tmp / "a" / "keep.txt", ec),
               "CleanupEmptyDirsUpTo must not touch files in non-empty ancestor");
        std::filesystem::remove_all(tmp, ec);
    }

    // CleanupEmptyDirsUpTo: no-op when startDir equals stopAt.
    {
        std::error_code ec;
        auto tmp = std::filesystem::temp_directory_path(ec) /
                   "cloudredirect_cleanup_test_3";
        std::filesystem::remove_all(tmp, ec);
        std::filesystem::create_directory(tmp, ec);

        FileUtil::CleanupEmptyDirsUpTo(tmp.string(), tmp.string());

        Expect(std::filesystem::exists(tmp, ec),
               "CleanupEmptyDirsUpTo must never remove the stopAt root itself");
        std::filesystem::remove_all(tmp, ec);
    }

    // CleanupEmptyDirsUpTo: no-op when startDir is outside stopAt.
    {
        std::error_code ec;
        auto tmpBase = std::filesystem::temp_directory_path(ec);
        auto stopRoot = tmpBase / "cloudredirect_cleanup_test_4_stop";
        auto outside  = tmpBase / "cloudredirect_cleanup_test_4_outside";
        std::filesystem::remove_all(stopRoot, ec);
        std::filesystem::remove_all(outside, ec);
        std::filesystem::create_directory(stopRoot, ec);
        std::filesystem::create_directory(outside, ec);

        FileUtil::CleanupEmptyDirsUpTo(outside.string(), stopRoot.string());

        Expect(std::filesystem::exists(outside, ec),
               "CleanupEmptyDirsUpTo must not touch dirs outside stopAt");
        std::filesystem::remove_all(stopRoot, ec);
        std::filesystem::remove_all(outside, ec);
    }

    return 0;
}
