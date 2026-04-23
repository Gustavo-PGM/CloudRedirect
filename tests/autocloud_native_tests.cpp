#include "local_storage.h"

#include <cstdlib>
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

    return 0;
}
