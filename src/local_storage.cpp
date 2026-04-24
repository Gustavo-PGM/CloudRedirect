#include "local_storage.h"
#include "file_util.h"
#include "log.h"
#include <wincrypt.h>
#include <ShlObj.h>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#pragma comment(lib, "Advapi32.lib")
#pragma comment(lib, "Shell32.lib")

namespace LocalStorage {

static std::string g_baseRoot;
static std::unordered_map<uint64_t, uint64_t> g_changeNumbers;
static std::shared_mutex g_mutex;

// Convert a file_time_type to Unix seconds using a single clock sample pair.
// Avoids the dual-now() jitter from sampling file_time_type::clock::now() and
// system_clock::now() at slightly different times.
static uint64_t FileTimeToUnixSeconds(std::filesystem::file_time_type ftime) {
    auto fileNow = std::filesystem::file_time_type::clock::now();
    auto sysNow = std::chrono::system_clock::now();
    auto sctp = std::chrono::time_point_cast<std::chrono::seconds>(
        ftime - fileNow + sysNow
    );
    return (uint64_t)sctp.time_since_epoch().count();
}

// Convert Unix seconds to file_time_type using a single clock sample pair.
static std::filesystem::file_time_type UnixSecondsToFileTime(uint64_t unixSeconds) {
    auto sysTime = std::chrono::system_clock::from_time_t((time_t)unixSeconds);
    auto sysNow = std::chrono::system_clock::now();
    auto fileNow = std::filesystem::file_time_type::clock::now();
    return fileNow + (sysTime - sysNow);
}

// Validate that a filename doesn't escape the app directory via path traversal.
// Returns the full path, or empty string if the filename is invalid.
static std::string ValidateFilename(const std::string& appRoot, const std::string& filename) {
    std::string fullPath = appRoot + filename;
    for (auto& c : fullPath) { if (c == '/') c = '\\'; }

    if (!FileUtil::IsPathWithin(appRoot, fullPath)) {
        LOG("BLOCKED path traversal: filename='%s' root='%s'",
            filename.c_str(), appRoot.c_str());
        return {};
    }

    return fullPath;
}

static uint64_t MakeKey(uint32_t accountId, uint32_t appId) {
    return ((uint64_t)accountId << 32) | appId;
}

static std::string GetAppPathInternal(uint32_t accountId, uint32_t appId) {
    return g_baseRoot + std::to_string(accountId) + "\\" + std::to_string(appId) + "\\";
}

static std::string ToLowerAscii(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return (char)std::tolower(c);
    });
    return s;
}

static std::string NormalizeSlashes(std::string s) {
    for (auto& c : s) { if (c == '\\') c = '/'; }
    return s;
}

static constexpr uintmax_t kMaxAppInfoBytes = 512ULL * 1024 * 1024;
static constexpr uint32_t kMaxAppInfoStrings = 200000;
static constexpr size_t kMaxAutoCloudScanFiles = 20000;
static constexpr int kMaxAutoCloudScanMillis = 5000;
static constexpr uint64_t kMaxAutoCloudCandidateBytes = 128ULL * 1024 * 1024;

static void ReplaceAll(std::string& s, const std::string& from, const std::string& to) {
    size_t pos = 0;
    while ((pos = s.find(from, pos)) != std::string::npos) {
        s.replace(pos, from.size(), to);
        pos += to.size();
    }
}

static std::string ExpandAutoCloudPathTokens(std::string path, uint32_t accountId) {
    const uint64_t steamId64Base = 76561197960265728ULL;
    const std::string accountIdStr = std::to_string(accountId);
    const std::string steamId64 = std::to_string(steamId64Base + accountId);
    ReplaceAll(path, "{Steam3AccountID}", accountIdStr);
    ReplaceAll(path, "{steam3accountid}", accountIdStr);
    ReplaceAll(path, "{64BitSteamID}", steamId64);
    ReplaceAll(path, "{64bitsteamid}", steamId64);
    ReplaceAll(path, "{SteamID64}", steamId64);
    ReplaceAll(path, "{steamid64}", steamId64);
    return path;
}

static bool IsSafeRelativePath(const std::string& path) {
    if (path.empty()) return true;
    if (path.find(':') != std::string::npos) return false;
    if (!path.empty() && (path.front() == '/' || path.front() == '\\')) return false;
    std::stringstream ss(NormalizeSlashes(path));
    std::string part;
    while (std::getline(ss, part, '/')) {
        if (part == "..") return false;
    }
    return true;
}

static std::string GetKnownFolderPathString(const KNOWNFOLDERID& id) {
    PWSTR wide = nullptr;
    if (FAILED(SHGetKnownFolderPath(id, KF_FLAG_DEFAULT, nullptr, &wide)) || !wide) return {};

    int len = WideCharToMultiByte(CP_UTF8, 0, wide, -1, nullptr, 0, nullptr, nullptr);
    std::string result;
    if (len > 1) {
        std::string tmp((size_t)len, '\0');
        WideCharToMultiByte(CP_UTF8, 0, wide, -1, tmp.data(), len, nullptr, nullptr);
        result.assign(tmp.data());
    }
    CoTaskMemFree(wide);
    return result;
}

static bool ReadU32(const std::vector<uint8_t>& data, size_t& offset, uint32_t& out) {
    if (offset + 4 > data.size()) return false;
    out = (uint32_t)data[offset] |
        ((uint32_t)data[offset + 1] << 8) |
        ((uint32_t)data[offset + 2] << 16) |
        ((uint32_t)data[offset + 3] << 24);
    offset += 4;
    return true;
}

static bool ReadI32(const std::vector<uint8_t>& data, size_t& offset, int32_t& out) {
    uint32_t u = 0;
    if (!ReadU32(data, offset, u)) return false;
    out = (int32_t)u;
    return true;
}

static std::string ReadCStringFromBytes(const std::vector<uint8_t>& data, size_t& offset) {
    size_t start = offset;
    while (offset < data.size() && data[offset] != 0) ++offset;
    std::string s(reinterpret_cast<const char*>(data.data() + start), offset - start);
    if (offset < data.size()) ++offset;
    return s;
}

struct AutoCloudRuleNative {
    std::string root;
    std::string path;
    std::string resolvedPath;
    std::string pattern;
    bool recursive = false;
    // Steam UFS platforms bitmask. -1 (all) when absent; 0 when explicitly empty.
    // Windows=1, MacOS=2, Linux=8 (see IDA dump of Steam platform table).
    uint32_t platforms = 0xFFFFFFFFu;
    std::vector<std::string> excludes;
};

static uint32_t ParseAutoCloudPlatformMask(const std::string& name) {
    std::string lower = ToLowerAscii(name);
    if (lower == "windows" || lower == "win") return 1;
    if (lower == "macos" || lower == "osx" || lower == "mac") return 2;
    if (lower == "linux") return 8;
    if (lower == "all") return 0xFFFFFFFFu;
    if (lower == "none") return 0;
    return 0;
}

static bool AutoCloudRuleMatchesCurrentPlatform(uint32_t mask) {
    // We run on Windows; Steam's Windows platform bit is 1.
    return (mask & 1u) != 0;
}

struct AutoCloudRootOverrideNative {
    std::string root;
    std::string os;
    std::string osCompare;
    std::string useInstead;
    std::string addPath;
    std::vector<std::pair<std::string, std::string>> pathTransforms;
};

struct AppInfoKVNode {
    std::string key;
    std::string stringValue;
    int32_t intValue = 0;
    bool hasString = false;
    bool hasInt = false;
    std::vector<AppInfoKVNode> children;
};

static std::vector<AppInfoKVNode> ParseAppInfoKV(const std::vector<uint8_t>& data, size_t& offset,
                                                 const std::vector<std::string>& strings, int depth = 0) {
    std::vector<AppInfoKVNode> nodes;
    if (depth >= 64) return nodes;

    while (offset < data.size()) {
        uint8_t type = data[offset++];
        if (type == 0x08 || type == 0x09) break;

        uint32_t keyIdx = 0;
        if (!ReadU32(data, offset, keyIdx)) break;

        AppInfoKVNode node;
        node.key = keyIdx < strings.size() ? strings[keyIdx] : "";

        switch (type) {
        case 0x00:
            node.children = ParseAppInfoKV(data, offset, strings, depth + 1);
            break;
        case 0x01:
            node.stringValue = ReadCStringFromBytes(data, offset);
            node.hasString = true;
            break;
        case 0x02:
            node.hasInt = ReadI32(data, offset, node.intValue);
            break;
        case 0x03:
        case 0x04:
        case 0x06:
            offset = offset + 4 > data.size() ? data.size() : offset + 4;
            break;
        case 0x07:
        case 0x0A:
            offset = offset + 8 > data.size() ? data.size() : offset + 8;
            break;
        case 0x05:
            ReadCStringFromBytes(data, offset);
            break;
        default:
            return nodes;
        }

        nodes.push_back(std::move(node));
    }

    return nodes;
}

static const AppInfoKVNode* FindChild(const std::vector<AppInfoKVNode>& nodes, const char* key) {
    for (const auto& node : nodes) {
        if (_stricmp(node.key.c_str(), key) == 0) return &node;
    }
    return nullptr;
}

static int WindowsVersionRank(std::string osName) {
    osName = ToLowerAscii(osName);
    if (osName == "windows11" || osName == "win11") return 11;
    if (osName == "windows10" || osName == "win10") return 10;
    if (osName == "windows8" || osName == "windows81" || osName == "win8" || osName == "win81") return 8;
    if (osName == "windows7" || osName == "win7") return 7;
    if (osName == "windows" || osName == "win") return 0;
    return -1;
}

static int CurrentWindowsVersionRank() {
    using RtlGetVersionFn = LONG (WINAPI *)(OSVERSIONINFOW*);
    auto fn = reinterpret_cast<RtlGetVersionFn>(GetProcAddress(GetModuleHandleA("ntdll.dll"), "RtlGetVersion"));
    OSVERSIONINFOW vi = {};
    vi.dwOSVersionInfoSize = sizeof(vi);
    if (fn && fn(&vi) == 0) {
        if (vi.dwMajorVersion >= 10) return vi.dwBuildNumber >= 22000 ? 11 : 10;
        if (vi.dwMajorVersion == 6 && vi.dwMinorVersion >= 2) return 8;
        if (vi.dwMajorVersion == 6 && vi.dwMinorVersion == 1) return 7;
    }
    return 10;
}

static bool IsWindowsRootOverrideActive(const AutoCloudRootOverrideNative& overrideRule) {
    int target = WindowsVersionRank(overrideRule.os);
    if (target < 0) return false;

    if (overrideRule.osCompare.empty() || _stricmp(overrideRule.osCompare.c_str(), "=") == 0) {
        return target == 0 || CurrentWindowsVersionRank() == target;
    }
    if (_stricmp(overrideRule.osCompare.c_str(), "<") == 0) {
        return target > 0 && CurrentWindowsVersionRank() < target;
    }
    return false;
}

static void ApplyRootOverridesForCurrentOS(AutoCloudRuleNative& rule,
                                           const std::vector<AutoCloudRootOverrideNative>& overrides) {
    for (const auto& overrideRule : overrides) {
        if (!IsWindowsRootOverrideActive(overrideRule)) continue;
        if (_stricmp(rule.root.c_str(), overrideRule.root.c_str()) != 0) continue;

        if (!overrideRule.useInstead.empty()) {
            rule.root = overrideRule.useInstead;
        }
        rule.resolvedPath = rule.path;
        for (const auto& [find, replace] : overrideRule.pathTransforms) {
            if (!find.empty()) ReplaceAll(rule.resolvedPath, find, replace);
        }
        if (!overrideRule.addPath.empty()) {
            std::string prefix = NormalizeSlashes(overrideRule.addPath);
            while (!prefix.empty() && prefix.back() == '/') prefix.pop_back();
            rule.resolvedPath = rule.resolvedPath.empty() ? prefix : prefix + "/" + rule.resolvedPath;
        }
        return;
    }
}

#ifdef CLOUDREDIRECT_TESTING
static bool WildcardMatchInsensitive(const std::string& pattern, const std::string& text);

bool TestResolveAutoCloudRootOverride(const std::string& root, const std::string& path,
                                      const std::string& overrideRoot,
                                      const std::string& useInstead,
                                      const std::string& addPath,
                                      const std::string& find,
                                      const std::string& replace,
                                      std::string& outRoot,
                                      std::string& outResolvedPath) {
    AutoCloudRuleNative rule;
    rule.root = root;
    rule.path = path;
    rule.resolvedPath = path;

    AutoCloudRootOverrideNative overrideRule;
    overrideRule.root = overrideRoot;
    overrideRule.os = "Windows";
    overrideRule.osCompare = "=";
    overrideRule.useInstead = useInstead;
    overrideRule.addPath = addPath;
    if (!find.empty()) overrideRule.pathTransforms.emplace_back(find, replace);

    ApplyRootOverridesForCurrentOS(rule, {overrideRule});
    outRoot = rule.root;
    outResolvedPath = rule.resolvedPath;
    return true;
}

bool TestIsSafeAutoCloudRelativePath(const std::string& path) {
    return IsSafeRelativePath(path);
}

bool TestParseMinimalAutoCloudKVFixture() {
    std::vector<std::string> strings = {
        "appinfo", "ufs", "savefiles", "0", "root", "path", "pattern", "recursive",
        "WinAppDataLocal", "Saves", "*.sav", "rootoverrides", "1", "os", "Windows",
        "oscompare", "=", "useinstead", "WinSavedGames", "addpath", "Migrated"
    };
    std::vector<uint8_t> data;
    auto u32 = [&](uint32_t v) {
        data.push_back((uint8_t)(v & 0xFF));
        data.push_back((uint8_t)((v >> 8) & 0xFF));
        data.push_back((uint8_t)((v >> 16) & 0xFF));
        data.push_back((uint8_t)((v >> 24) & 0xFF));
    };
    auto section = [&](uint32_t key) { data.push_back(0x00); u32(key); };
    auto str = [&](uint32_t key, const char* value) {
        data.push_back(0x01); u32(key);
        data.insert(data.end(), value, value + strlen(value) + 1);
    };
    auto i32 = [&](uint32_t key, int32_t value) {
        data.push_back(0x02); u32(key); u32((uint32_t)value);
    };
    auto end = [&]() { data.push_back(0x08); };

    section(0);      // appinfo
    section(1);      // ufs
    section(2);      // savefiles
    section(3);      // 0
    str(4, "WinAppDataLocal");
    str(5, "Saves");
    str(6, "*.sav");
    i32(7, 1);
    end();
    end();        // savefiles
    section(11);  // rootoverrides
    section(12);  // 1
    str(4, "WinAppDataLocal");
    str(13, "Windows");
    str(15, "=");
    str(17, "WinSavedGames");
    str(19, "Migrated");
    end(); end(); end(); end();

    size_t offset = 0;
    auto tree = ParseAppInfoKV(data, offset, strings);
    const auto* appInfo = FindChild(tree, "appinfo");
    const auto* ufs = appInfo ? FindChild(appInfo->children, "ufs") : nullptr;
    const auto* savefiles = ufs ? FindChild(ufs->children, "savefiles") : nullptr;
    const auto* entry = savefiles && !savefiles->children.empty() ? &savefiles->children.front() : nullptr;
    const auto* root = entry ? FindChild(entry->children, "root") : nullptr;
    const auto* path = entry ? FindChild(entry->children, "path") : nullptr;
    const auto* pattern = entry ? FindChild(entry->children, "pattern") : nullptr;
    const auto* recursive = entry ? FindChild(entry->children, "recursive") : nullptr;
    const auto* rootoverrides = ufs ? FindChild(ufs->children, "rootoverrides") : nullptr;
    const auto* overrideEntry = rootoverrides && !rootoverrides->children.empty() ? &rootoverrides->children.front() : nullptr;
    AutoCloudRootOverrideNative overrideRule;
    if (overrideEntry) {
        const auto* overrideRoot = FindChild(overrideEntry->children, "root");
        const auto* os = FindChild(overrideEntry->children, "os");
        const auto* osCompare = FindChild(overrideEntry->children, "oscompare");
        const auto* useInstead = FindChild(overrideEntry->children, "useinstead");
        const auto* addPath = FindChild(overrideEntry->children, "addpath");
        overrideRule.root = overrideRoot && overrideRoot->hasString ? overrideRoot->stringValue : "";
        overrideRule.os = os && os->hasString ? os->stringValue : "";
        overrideRule.osCompare = osCompare && osCompare->hasString ? osCompare->stringValue : "";
        overrideRule.useInstead = useInstead && useInstead->hasString ? useInstead->stringValue : "";
        overrideRule.addPath = addPath && addPath->hasString ? addPath->stringValue : "";
    }
    AutoCloudRuleNative rule;
    rule.root = root && root->hasString ? root->stringValue : "";
    rule.path = path && path->hasString ? path->stringValue : "";
    rule.resolvedPath = rule.path;
    ApplyRootOverridesForCurrentOS(rule, {overrideRule});
    return root && root->stringValue == "WinAppDataLocal" &&
        path && path->stringValue == "Saves" &&
        pattern && pattern->stringValue == "*.sav" &&
        recursive && recursive->hasInt && recursive->intValue == 1 &&
        rule.root == "WinSavedGames" && rule.resolvedPath == "Migrated/Saves";
}

bool TestAutoCloudPlatformAndExcludeFilters() {
    // String table: indices referenced below.
    std::vector<std::string> strings = {
        "root", "path", "pattern", "recursive",   // 0-3
        "platforms", "exclude",                    // 4-5
        "Windows", "Linux",                        // 6-7
        "WinAppDataLocal", "Saves", "*",           // 8-10
        "*.log"                                    // 11
    };

    std::vector<uint8_t> data;
    auto u32 = [&](uint32_t v) {
        data.push_back((uint8_t)(v & 0xFF));
        data.push_back((uint8_t)((v >> 8) & 0xFF));
        data.push_back((uint8_t)((v >> 16) & 0xFF));
        data.push_back((uint8_t)((v >> 24) & 0xFF));
    };
    auto section = [&](uint32_t key) { data.push_back(0x00); u32(key); };
    auto str = [&](uint32_t key, uint32_t valueIdx) {
        data.push_back(0x01); u32(key);
        const std::string& v = strings[valueIdx];
        data.insert(data.end(), v.begin(), v.end());
        data.push_back(0x00);
    };
    auto i32 = [&](uint32_t key, int32_t value) {
        data.push_back(0x02); u32(key); u32((uint32_t)value);
    };
    auto end = [&]() { data.push_back(0x08); };

    // Build rule with root/path/pattern/recursive, platforms=[Windows], exclude=[*.log].
    str(0, 8);     // root = "WinAppDataLocal"
    str(1, 9);     // path = "Saves"
    str(2, 10);    // pattern = "*"
    i32(3, 1);     // recursive = 1
    section(4);    // platforms {
    str(0, 6);     //   [anon] = "Windows"
    end();         // }
    section(5);    // exclude {
    str(0, 11);    //   [anon] = "*.log"
    end();         // }

    size_t offset = 0;
    auto tree = ParseAppInfoKV(data, offset, strings);

    AutoCloudRuleNative rule;
    const auto* root = FindChild(tree, "root");
    const auto* path = FindChild(tree, "path");
    const auto* pattern = FindChild(tree, "pattern");
    const auto* recursive = FindChild(tree, "recursive");
    const auto* platforms = FindChild(tree, "platforms");
    const auto* excludes = FindChild(tree, "exclude");
    rule.root = root && root->hasString ? root->stringValue : "";
    rule.path = path && path->hasString ? path->stringValue : "";
    rule.pattern = pattern && pattern->hasString ? pattern->stringValue : "*";
    rule.recursive = recursive && recursive->hasInt && recursive->intValue != 0;
    if (platforms) {
        uint32_t mask = 0;
        for (const auto& plat : platforms->children) {
            if (plat.hasString) mask |= ParseAutoCloudPlatformMask(plat.stringValue);
        }
        rule.platforms = mask;
    }
    if (excludes) {
        for (const auto& ex : excludes->children) {
            if (ex.hasString && !ex.stringValue.empty()) rule.excludes.push_back(ex.stringValue);
        }
    }

    // Rule parsed with Windows-only platform mask and exclude "*.log".
    bool basics = rule.root == "WinAppDataLocal" && rule.path == "Saves" &&
        rule.pattern == "*" && rule.recursive &&
        rule.platforms == 1u && rule.excludes.size() == 1 &&
        rule.excludes.front() == "*.log";
    if (!basics) return false;

    // Windows rule matches current platform; a Linux-only mask (8) would not.
    if (!AutoCloudRuleMatchesCurrentPlatform(rule.platforms)) return false;
    if (AutoCloudRuleMatchesCurrentPlatform(8u)) return false;

    // Exclude pattern "*.log" drops log files but keeps save files.
    auto excluded = [&](const char* leaf) {
        for (const auto& ex : rule.excludes) {
            if (WildcardMatchInsensitive(ex, std::string(leaf))) return true;
        }
        return false;
    };
    if (!excluded("debug.log")) return false;
    if (excluded("slot1.sav")) return false;

    return true;
}
#endif

static std::vector<AutoCloudRuleNative> LoadAutoCloudRules(const std::string& steamPath, uint32_t appId) {
    std::vector<AutoCloudRuleNative> rules;
    std::filesystem::path appInfoPath = std::filesystem::path(steamPath) / "appcache" / "appinfo.vdf";
    std::error_code statEc;
    auto appInfoMtime = std::filesystem::last_write_time(appInfoPath, statEc);
    auto appInfoSize = std::filesystem::file_size(appInfoPath, statEc);
    if (statEc) {
        LOG("GetAutoCloudFileList: failed to stat appinfo.vdf: %s", statEc.message().c_str());
        return rules;
    }
    if (appInfoSize > kMaxAppInfoBytes) {
        LOG("GetAutoCloudFileList: appinfo.vdf too large: %llu bytes", (unsigned long long)appInfoSize);
        return rules;
    }

    struct RulesCacheEntry {
        std::filesystem::file_time_type mtime;
        uintmax_t size = 0;
        std::vector<AutoCloudRuleNative> rules;
    };
    static std::mutex cacheMutex;
    static std::unordered_map<std::string, RulesCacheEntry> cache;
    std::string cacheKey = appInfoPath.string() + "\n" + std::to_string(appId);
    {
        std::lock_guard<std::mutex> lock(cacheMutex);
        auto it = cache.find(cacheKey);
        if (it != cache.end() && it->second.mtime == appInfoMtime && it->second.size == appInfoSize) {
            return it->second.rules;
        }
    }

    auto cacheRules = [&](const std::vector<AutoCloudRuleNative>& parsedRules) {
        std::lock_guard<std::mutex> lock(cacheMutex);
        cache[cacheKey] = RulesCacheEntry{appInfoMtime, appInfoSize, parsedRules};
    };

    std::ifstream f(appInfoPath, std::ios::binary | std::ios::ate);
    if (!f) {
        LOG("GetAutoCloudFileList: appinfo.vdf not found: %s", appInfoPath.string().c_str());
        return rules;
    }

    auto fileSize = f.tellg();
    if (fileSize < 16) return rules;
    if (static_cast<uintmax_t>(fileSize) > kMaxAppInfoBytes) {
        LOG("GetAutoCloudFileList: appinfo.vdf too large after open: %llu bytes",
            (unsigned long long)fileSize);
        return rules;
    }
    f.seekg(0, std::ios::beg);
    std::vector<uint8_t> bytes((size_t)fileSize);
    if (!f.read(reinterpret_cast<char*>(bytes.data()), fileSize)) return rules;

    size_t offset = 0;
    uint32_t magic = 0, universe = 0, stringOffsetLo = 0, stringOffsetHi = 0;
    if (!ReadU32(bytes, offset, magic) || !ReadU32(bytes, offset, universe) ||
        !ReadU32(bytes, offset, stringOffsetLo) || !ReadU32(bytes, offset, stringOffsetHi)) {
        return rules;
    }
    uint64_t stringOffset = ((uint64_t)stringOffsetHi << 32) | stringOffsetLo;
    if (magic != 0x07564429 || stringOffset >= bytes.size()) {
        LOG("GetAutoCloudFileList: unsupported appinfo.vdf format magic=0x%08X", magic);
        return rules;
    }

    size_t stringTableOffset = (size_t)stringOffset;
    size_t st = stringTableOffset;
    uint32_t stringCount = 0;
    if (!ReadU32(bytes, st, stringCount)) return rules;
    size_t remainingStringBytes = bytes.size() - st;
    if (stringCount > remainingStringBytes || stringCount > kMaxAppInfoStrings) {
        LOG("GetAutoCloudFileList: invalid appinfo string count: %u", stringCount);
        return rules;
    }

    std::vector<std::string> strings;
    strings.reserve(stringCount);
    for (uint32_t i = 0; i < stringCount && st < bytes.size(); ++i) {
        strings.push_back(ReadCStringFromBytes(bytes, st));
    }

    offset = 16;
    while (offset + 8 <= stringTableOffset) {
        uint32_t recordAppId = 0, size = 0;
        if (!ReadU32(bytes, offset, recordAppId)) break;
        if (recordAppId == 0) break;
        if (!ReadU32(bytes, offset, size)) break;
        if (size == 0 || offset + size > stringTableOffset) break;

        if (recordAppId != appId) {
            offset += size;
            continue;
        }

        if (size < 60) return rules;
        std::vector<uint8_t> kv(bytes.begin() + offset + 60, bytes.begin() + offset + size);
        size_t kvOffset = 0;
        auto tree = ParseAppInfoKV(kv, kvOffset, strings);
        const auto* appInfo = FindChild(tree, "appinfo");
        if (!appInfo) return rules;
        const auto* ufs = FindChild(appInfo->children, "ufs");
        if (!ufs) return rules;
        const auto* savefiles = FindChild(ufs->children, "savefiles");
        if (!savefiles) return rules;

        std::vector<AutoCloudRootOverrideNative> overrides;
        const auto* rootoverrides = FindChild(ufs->children, "rootoverrides");
        if (rootoverrides) {
            for (const auto& entry : rootoverrides->children) {
                AutoCloudRootOverrideNative overrideRule;
                const auto* root = FindChild(entry.children, "root");
                const auto* os = FindChild(entry.children, "os");
                const auto* osCompare = FindChild(entry.children, "oscompare");
                const auto* useInstead = FindChild(entry.children, "useinstead");
                const auto* addPath = FindChild(entry.children, "addpath");
                overrideRule.root = root && root->hasString ? root->stringValue : "";
                overrideRule.os = os && os->hasString ? os->stringValue : "";
                overrideRule.osCompare = osCompare && osCompare->hasString ? osCompare->stringValue : "";
                overrideRule.useInstead = useInstead && useInstead->hasString ? useInstead->stringValue : "";
                overrideRule.addPath = addPath && addPath->hasString ? addPath->stringValue : "";

                const auto* transforms = FindChild(entry.children, "pathtransforms");
                if (transforms) {
                    for (const auto& transform : transforms->children) {
                        const auto* find = FindChild(transform.children, "find");
                        const auto* replace = FindChild(transform.children, "replace");
                        overrideRule.pathTransforms.emplace_back(
                            find && find->hasString ? find->stringValue : "",
                            replace && replace->hasString ? replace->stringValue : "");
                    }
                }

                if (!overrideRule.root.empty() &&
                    (!overrideRule.useInstead.empty() || !overrideRule.addPath.empty() ||
                     !overrideRule.pathTransforms.empty())) {
                    overrides.push_back(std::move(overrideRule));
                }
            }
        }

        for (const auto& entry : savefiles->children) {
            AutoCloudRuleNative rule;
            const auto* root = FindChild(entry.children, "root");
            const auto* path = FindChild(entry.children, "path");
            const auto* pattern = FindChild(entry.children, "pattern");
            const auto* recursive = FindChild(entry.children, "recursive");
            rule.root = root && root->hasString ? root->stringValue : "";
            rule.path = path && path->hasString ? path->stringValue : "";
            rule.resolvedPath = rule.path;
            rule.pattern = pattern && pattern->hasString ? pattern->stringValue : "*";
            rule.recursive = recursive && recursive->hasInt && recursive->intValue != 0;

            // platforms: optional sub-section of platform names (e.g. "Windows", "Linux").
            // Absent = match all; present = bitmask OR of listed platforms.
            const auto* platforms = FindChild(entry.children, "platforms");
            if (platforms) {
                uint32_t mask = 0;
                for (const auto& plat : platforms->children) {
                    if (plat.hasString) mask |= ParseAutoCloudPlatformMask(plat.stringValue);
                }
                rule.platforms = mask;
            }

            const auto* excludes = FindChild(entry.children, "exclude");
            if (excludes) {
                for (const auto& ex : excludes->children) {
                    if (ex.hasString && !ex.stringValue.empty()) {
                        rule.excludes.push_back(ex.stringValue);
                    }
                }
            }

            ApplyRootOverridesForCurrentOS(rule, overrides);
            rules.push_back(std::move(rule));
        }
        cacheRules(rules);
        return rules;
    }

    cacheRules(rules);
    return rules;
}

static bool WildcardMatchInsensitive(const char* pattern, const char* text) {
    while (*pattern) {
        if (*pattern == '*') {
            while (*pattern == '*') ++pattern;
            if (!*pattern) return true;
            while (*text && *text != '/') {
                if (WildcardMatchInsensitive(pattern, text)) return true;
                ++text;
            }
            return false;
        }
        if (!*text) return false;
        if (*text == '/' && *pattern != '/') return false;
        if (*pattern != '?' && std::tolower((unsigned char)*pattern) != std::tolower((unsigned char)*text)) {
            return false;
        }
        ++pattern;
        ++text;
    }
    return *text == 0;
}

static bool WildcardMatchInsensitive(const std::string& pattern, const std::string& text) {
    return WildcardMatchInsensitive(pattern.c_str(), text.c_str());
}

static std::vector<std::filesystem::path> GetSteamLibraryPaths(const std::string& steamPath) {
    std::vector<std::filesystem::path> paths;
    paths.push_back(std::filesystem::path(steamPath));

    std::ifstream f(std::filesystem::path(steamPath) / "config" / "libraryfolders.vdf");
    if (!f) return paths;

    std::string line;
    while (std::getline(f, line)) {
        if (line.find("\"path\"") == std::string::npos) continue;
        auto first = line.find('"', line.find("\"path\"") + 6);
        if (first == std::string::npos) continue;
        auto second = line.find('"', first + 1);
        if (second == std::string::npos) continue;
        std::string path = line.substr(first + 1, second - first - 1);
        size_t pos = 0;
        while ((pos = path.find("\\\\", pos)) != std::string::npos) {
            path.replace(pos, 2, "\\");
            ++pos;
        }
        std::filesystem::path p(path);
        if (!std::filesystem::exists(p)) continue;
        bool seen = false;
        for (const auto& existing : paths) {
            if (_stricmp(existing.string().c_str(), p.string().c_str()) == 0) {
                seen = true;
                break;
            }
        }
        if (!seen) paths.push_back(std::move(p));
    }

    return paths;
}

static std::string FindGameInstallPath(const std::string& steamPath, uint32_t appId) {
    for (const auto& libPath : GetSteamLibraryPaths(steamPath)) {
        auto manifestPath = libPath / "steamapps" / ("appmanifest_" + std::to_string(appId) + ".acf");
        std::ifstream mf(manifestPath);
        if (!mf) continue;

        std::string line;
        while (std::getline(mf, line)) {
            auto pos = line.find("\"installdir\"");
            if (pos == std::string::npos) continue;
            auto q1 = line.rfind('"');
            auto q2 = q1 == std::string::npos ? std::string::npos : line.rfind('"', q1 - 1);
            if (q1 != std::string::npos && q2 != std::string::npos && q1 > q2) {
                auto installDir = line.substr(q2 + 1, q1 - q2 - 1);
                return (libPath / "steamapps" / "common" / installDir).string();
            }
        }
    }
    return {};
}

// Persist change number to disk (must be called while holding g_mutex)
static void SaveChangeNumberLocked(uint32_t accountId, uint32_t appId) {
    auto key = MakeKey(accountId, appId);
    auto it = g_changeNumbers.find(key);
    if (it == g_changeNumbers.end()) return;

    std::string cnPath = GetAppPathInternal(accountId, appId) + "cn.dat";
    if (FileUtil::AtomicWriteText(cnPath, std::to_string(it->second))) {
        LOG("SaveChangeNumber: persisted CN=%llu for app %u", it->second, appId);
    } else {
        LOG("SaveChangeNumber: failed to persist CN for app %u", appId);
    }
}

void Init(const std::string& baseRoot) {
    g_baseRoot = baseRoot;
    if (!g_baseRoot.empty() && g_baseRoot.back() != '\\')
        g_baseRoot += '\\';
    std::filesystem::create_directories(g_baseRoot);
    LOG("LocalStorage initialized at: %s", g_baseRoot.c_str());
}

void InitApp(uint32_t accountId, uint32_t appId) {
    auto appPath = GetAppPathInternal(accountId, appId);
    std::filesystem::create_directories(appPath);
    LOG("LocalStorage: account %u app %u path: %s", accountId, appId, appPath.c_str());
}

std::string GetAppPath(uint32_t accountId, uint32_t appId) {
    return GetAppPathInternal(accountId, appId);
}

uint64_t GetChangeNumber(uint32_t accountId, uint32_t appId) {
    // Fast path: shared lock for cache reads (common case)
    {
        std::shared_lock<std::shared_mutex> rlock(g_mutex);
        auto key = MakeKey(accountId, appId);
        auto it = g_changeNumbers.find(key);
        if (it != g_changeNumbers.end()) return it->second;
    }

    // Slow path: exclusive lock for disk load + cache insert
    std::lock_guard<std::shared_mutex> lock(g_mutex);
    auto key = MakeKey(accountId, appId);
    // Re-check under exclusive lock (another thread may have loaded it)
    auto it = g_changeNumbers.find(key);
    if (it != g_changeNumbers.end()) return it->second;

    // Load from disk if not in memory
    std::string cnPath = GetAppPathInternal(accountId, appId) + "cn.dat";
    std::ifstream f(cnPath);
    if (f) {
        uint64_t cn = 0;
        f >> cn;
        if (cn > 0) {
            g_changeNumbers[key] = cn;
            LOG("GetChangeNumber: loaded CN=%llu from disk for app %u", cn, appId);
            return cn;
        }
    }

    // Default: if we have any files, start at 1
    g_changeNumbers[key] = 1;
    return 1;
}

void SetChangeNumber(uint32_t accountId, uint32_t appId, uint64_t cn) {
    std::lock_guard<std::shared_mutex> lock(g_mutex);
    auto key = MakeKey(accountId, appId);
    g_changeNumbers[key] = cn;
    SaveChangeNumberLocked(accountId, appId);
    LOG("SetChangeNumber: CN=%llu for app %u", cn, appId);
}

uint64_t IncrementChangeNumber(uint32_t accountId, uint32_t appId) {
    std::lock_guard<std::shared_mutex> lock(g_mutex);
    auto key = MakeKey(accountId, appId);
    auto it = g_changeNumbers.find(key);
    if (it == g_changeNumbers.end()) {
        // Load from disk first if not in memory
        std::string cnPath = GetAppPathInternal(accountId, appId) + "cn.dat";
        std::ifstream f(cnPath);
        uint64_t cn = 1;
        if (f) { f >> cn; if (cn == 0) cn = 1; }
        g_changeNumbers[key] = cn;
    }
    uint64_t newCN = ++g_changeNumbers[key];
    SaveChangeNumberLocked(accountId, appId);
    return newCN;
}

std::vector<uint8_t> SHA1(const uint8_t* data, size_t len) {
    std::vector<uint8_t> hash(20, 0);
    HCRYPTPROV hProv = 0;
    HCRYPTHASH hHash = 0;
    if (CryptAcquireContextW(&hProv, nullptr, nullptr, PROV_RSA_FULL, CRYPT_VERIFYCONTEXT)) {
        if (CryptCreateHash(hProv, CALG_SHA1, 0, 0, &hHash)) {
            CryptHashData(hHash, data, (DWORD)len, 0);
            DWORD hashLen = 20;
            CryptGetHashParam(hHash, HP_HASHVAL, hash.data(), &hashLen, 0);
            CryptDestroyHash(hHash);
        }
        CryptReleaseContext(hProv, 0);
    }
    return hash;
}

// Streaming SHA1 — reads file in chunks to avoid loading entire file into memory
static std::vector<uint8_t> SHA1File(const std::string& path) {
    std::vector<uint8_t> hash(20, 0);
    std::ifstream f(path, std::ios::binary);
    if (!f) return hash;

    HCRYPTPROV hProv = 0;
    HCRYPTHASH hHash = 0;
    if (CryptAcquireContextW(&hProv, nullptr, nullptr, PROV_RSA_FULL, CRYPT_VERIFYCONTEXT)) {
        if (CryptCreateHash(hProv, CALG_SHA1, 0, 0, &hHash)) {
            char buf[65536];
            while (f.read(buf, sizeof(buf)) || f.gcount() > 0) {
                CryptHashData(hHash, (const BYTE*)buf, (DWORD)f.gcount(), 0);
                if (f.eof()) break;
            }
            DWORD hashLen = 20;
            CryptGetHashParam(hHash, HP_HASHVAL, hash.data(), &hashLen, 0);
            CryptDestroyHash(hHash);
        }
        CryptReleaseContext(hProv, 0);
    }
    return hash;
}

std::vector<FileEntry> GetFileList(uint32_t accountId, uint32_t appId) {
    std::vector<FileEntry> result;

    // Phase 1: Collect file paths under shared lock (fast — no hashing)
    struct PendingFile {
        std::string relPath;
        std::string fullPath;
        uint64_t rawSize;
        uint64_t timestamp;
    };
    std::vector<PendingFile> pending;

    {
        std::shared_lock<std::shared_mutex> lock(g_mutex);
        std::string appRoot = GetAppPathInternal(accountId, appId);
        if (!std::filesystem::exists(appRoot)) return result;

        for (auto& entry : std::filesystem::recursive_directory_iterator(appRoot)) {
            if (!entry.is_regular_file()) continue;

            std::string relPath = std::filesystem::relative(entry.path(), appRoot).string();
            for (auto& c : relPath) { if (c == '\\') c = '/'; }

            // Skip our internal metadata files
            if (relPath == "cn.dat" || relPath == "root_token.dat" ||
                relPath == "file_tokens.dat" || relPath == "deleted.dat") continue;

            std::string fullPath = appRoot + relPath;
            for (auto& c : fullPath) { if (c == '/') c = '\\'; }

            auto ftime = std::filesystem::last_write_time(entry.path());
            uint64_t ts = FileTimeToUnixSeconds(ftime);

            PendingFile pf;
            pf.relPath = std::move(relPath);
            pf.fullPath = std::move(fullPath);
            pf.rawSize = (uint64_t)entry.file_size();
            pf.timestamp = ts;
            pending.push_back(std::move(pf));
        }
    }
    // Lock released — hash files without blocking writers

    // Phase 2: Compute SHA1 hashes without holding the lock
    for (auto& pf : pending) {
        auto sha = SHA1File(pf.fullPath);
        // File may have been deleted between phase 1 and 2 — skip if hash is empty/all zeros
        if (sha.empty() || std::all_of(sha.begin(), sha.end(), [](uint8_t b) { return b == 0; }))
            continue;

        FileEntry fe;
        fe.filename = std::move(pf.relPath);
        fe.sha = sha;
        fe.timestamp = pf.timestamp;
        fe.rawSize = pf.rawSize;
        fe.deleted = false;
        fe.rootId = 0;
        result.push_back(std::move(fe));
    }

    return result;
}

std::optional<FileEntry> GetFileEntry(uint32_t accountId, uint32_t appId, const std::string& filename) {
    // Phase 1: resolve path and check existence under shared lock
    std::string fullPath;
    {
        std::shared_lock<std::shared_mutex> lock(g_mutex);
        std::string appRoot = GetAppPathInternal(accountId, appId);
        fullPath = appRoot + filename;
        for (auto& c : fullPath) { if (c == '/') c = '\\'; }

        if (!std::filesystem::exists(fullPath) || !std::filesystem::is_regular_file(fullPath))
            return std::nullopt;
    }

    // Phase 2: SHA1 hash and stat outside the lock (expensive I/O)
    // File may be deleted between Phase 1 and Phase 2 (TOCTOU) -- catch exceptions.
    try {
        auto sha = SHA1File(fullPath);
        auto ftime = std::filesystem::last_write_time(fullPath);
        uint64_t ts = FileTimeToUnixSeconds(ftime);

        FileEntry fe;
        fe.filename = filename;
        fe.sha = sha;
        fe.timestamp = ts;
        fe.rawSize = (uint64_t)std::filesystem::file_size(fullPath);
        fe.deleted = false;
        fe.rootId = 0;
        return fe;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::vector<uint8_t> ReadFile(uint32_t accountId, uint32_t appId, const std::string& filename) {
    std::shared_lock<std::shared_mutex> lock(g_mutex);

    std::string appRoot = GetAppPathInternal(accountId, appId);
    std::string fullPath = ValidateFilename(appRoot, filename);
    if (fullPath.empty()) return {};

    std::ifstream f(fullPath, std::ios::binary);
    if (!f) return {};
    return std::vector<uint8_t>(
        std::istreambuf_iterator<char>(f),
        std::istreambuf_iterator<char>()
    );
}

bool WriteFile(uint32_t accountId, uint32_t appId, const std::string& filename, const uint8_t* data, size_t len) {
    std::lock_guard<std::shared_mutex> lock(g_mutex);

    std::string appRoot = GetAppPathInternal(accountId, appId);
    std::string fullPath = ValidateFilename(appRoot, filename);
    if (fullPath.empty()) {
        LOG("WriteFile BLOCKED: path traversal in filename '%s'", filename.c_str());
        return false;
    }

    auto parent = std::filesystem::path(fullPath).parent_path();
    std::filesystem::create_directories(parent);

    // Atomic write: write to .tmp then rename to avoid partial reads on crash
    if (!FileUtil::AtomicWriteBinary(fullPath, data, len)) {
        LOG("WriteFile failed: %s (%zu bytes)", fullPath.c_str(), len);
        return false;
    }
    ++g_changeNumbers[MakeKey(accountId, appId)];
    SaveChangeNumberLocked(accountId, appId);
    LOG("WriteFile: app %u %s (%zu bytes)", appId, filename.c_str(), len);
    return true;
}

bool WriteFileNoIncrement(uint32_t accountId, uint32_t appId, const std::string& filename, const uint8_t* data, size_t len) {
    std::lock_guard<std::shared_mutex> lock(g_mutex);

    std::string appRoot = GetAppPathInternal(accountId, appId);
    std::string fullPath = ValidateFilename(appRoot, filename);
    if (fullPath.empty()) {
        LOG("WriteFileNoIncrement BLOCKED: path traversal in filename '%s'", filename.c_str());
        return false;
    }

    auto parent = std::filesystem::path(fullPath).parent_path();
    std::filesystem::create_directories(parent);

    if (!FileUtil::AtomicWriteBinary(fullPath, data, len)) {
        LOG("WriteFileNoIncrement failed: %s (%zu bytes)", fullPath.c_str(), len);
        return false;
    }
    LOG("WriteFileNoIncrement: app %u %s (%zu bytes)", appId, filename.c_str(), len);
    return true;
}

bool DeleteFile(uint32_t accountId, uint32_t appId, const std::string& filename) {
    std::lock_guard<std::shared_mutex> lock(g_mutex);

    std::string appRoot = GetAppPathInternal(accountId, appId);
    std::string fullPath = ValidateFilename(appRoot, filename);
    if (fullPath.empty()) {
        LOG("DeleteFile BLOCKED: path traversal in filename '%s'", filename.c_str());
        return false;
    }

    if (std::filesystem::remove(fullPath)) {
        ++g_changeNumbers[MakeKey(accountId, appId)];
        SaveChangeNumberLocked(accountId, appId);
        LOG("DeleteFile: app %u %s", appId, filename.c_str());
        return true;
    }
    return false;
}

bool RestoreFileIfUnchanged(uint32_t accountId, uint32_t appId,
                            const std::string& filename,
                            const std::vector<uint8_t>& expectedData,
                            const std::string& backupPath,
                            bool hadOriginal) {
    std::lock_guard<std::shared_mutex> lock(g_mutex);

    std::string appRoot = GetAppPathInternal(accountId, appId);
    std::string fullPath = ValidateFilename(appRoot, filename);
    if (fullPath.empty()) {
        LOG("RestoreFileIfUnchanged BLOCKED: path traversal in filename '%s'", filename.c_str());
        return false;
    }

    std::vector<uint8_t> currentData;
    {
        std::ifstream f(fullPath, std::ios::binary);
        if (f) {
            currentData.assign(std::istreambuf_iterator<char>(f),
                               std::istreambuf_iterator<char>());
        } else if (hadOriginal) {
            // File missing but we expected to restore an original -- skip, do not clobber.
            LOG("RestoreFileIfUnchanged: %s missing, expected to restore original; skipping",
                filename.c_str());
            return false;
        }
    }
    if (currentData != expectedData) {
        LOG("RestoreFileIfUnchanged: %s modified concurrently; skipping rollback",
            filename.c_str());
        return false;
    }

    std::error_code ec;
    if (hadOriginal) {
        std::filesystem::copy_file(backupPath, fullPath,
                                   std::filesystem::copy_options::overwrite_existing, ec);
        if (ec) {
            LOG("RestoreFileIfUnchanged: copy failed for %s: %s",
                filename.c_str(), ec.message().c_str());
            return false;
        }
    } else {
        std::filesystem::remove(fullPath, ec);
        if (ec) {
            LOG("RestoreFileIfUnchanged: remove failed for %s: %s",
                filename.c_str(), ec.message().c_str());
            return false;
        }
    }
    LOG("RestoreFileIfUnchanged: %s restored (hadOriginal=%d)", filename.c_str(), hadOriginal ? 1 : 0);
    return true;
}

bool SetFileTimestamp(uint32_t accountId, uint32_t appId, const std::string& filename, uint64_t unixSeconds) {
    if (unixSeconds == 0) return false;
    std::lock_guard<std::shared_mutex> lock(g_mutex);

    std::string appRoot = GetAppPathInternal(accountId, appId);
    std::string fullPath = ValidateFilename(appRoot, filename);
    if (fullPath.empty()) {
        LOG("SetFileTimestamp BLOCKED: path traversal in filename '%s'", filename.c_str());
        return false;
    }

    auto fileTime = UnixSecondsToFileTime(unixSeconds);

    std::error_code ec;
    std::filesystem::last_write_time(fullPath, fileTime, ec);
    if (ec) {
        LOG("SetFileTimestamp: failed for %s: %s", filename.c_str(), ec.message().c_str());
        return false;
    }
    LOG("SetFileTimestamp: %s -> %llu", filename.c_str(), unixSeconds);
    return true;
}


std::vector<FileEntry> GetAutoCloudFileList(const std::string& steamPath, uint32_t accountId, uint32_t appId) {
    std::vector<FileEntry> result;

    auto rules = LoadAutoCloudRules(steamPath, appId);
    if (rules.empty()) {
        LOG("GetAutoCloudFileList: no appinfo UFS save rules for app %u", appId);
        return result;
    }

    std::filesystem::path appUserdataDir = std::filesystem::path(steamPath) / "userdata" /
        std::to_string(accountId) / std::to_string(appId);

    auto addFile = [&](const std::filesystem::directory_entry& fileEntry,
                       const std::string& cloudPath,
                       const std::string& sourcePath,
                       const std::string& rootToken,
                       uint32_t rootId) {
        std::string fileName = fileEntry.path().filename().string();
        if (fileName == "steam_autocloud.vdf") return;

        std::error_code ec;
        uint64_t rawSize = (uint64_t)fileEntry.file_size(ec);
        if (ec) return;
        if (rawSize > kMaxAutoCloudCandidateBytes) {
            LOG("GetAutoCloudFileList: skipping oversized app %u candidate %s (%llu bytes)",
                appId, sourcePath.c_str(), (unsigned long long)rawSize);
            return;
        }

        auto sha = SHA1File(fileEntry.path().string());
        auto ftime = std::filesystem::last_write_time(fileEntry.path(), ec);
        if (ec) return;
        uint64_t ts = FileTimeToUnixSeconds(ftime);

        FileEntry fe;
        fe.filename = cloudPath;
        fe.sourcePath = sourcePath;
        fe.rootToken = rootToken;
        fe.sha = sha;
        fe.timestamp = ts;
        fe.rawSize = rawSize;
        fe.deleted = false;
        fe.rootId = rootId;
        result.push_back(std::move(fe));
    };

    struct RootMapping {
        std::string dirName;
        std::string rootToken;
        uint32_t rootId;
        std::string envExpansion;
    };
    // Thread-safe environment variable expansion using GetEnvironmentVariableA
    // (getenv() returns a pointer to an internal static buffer that is not thread-safe)
    auto getEnvSafe = [](const char* name, char* buf, size_t bufSize) -> bool {
        DWORD n = GetEnvironmentVariableA(name, buf, (DWORD)bufSize);
        return n > 0 && n < (DWORD)bufSize;
    };

    char localLow[MAX_PATH] = {};
    {
        char tmp[MAX_PATH];
        if (getEnvSafe("LOCALAPPDATA", tmp, sizeof(tmp)))
            snprintf(localLow, sizeof(localLow), "%s\\..\\LocalLow\\", tmp);
    }

    char localAppData[MAX_PATH] = {};
    {
        char tmp[MAX_PATH];
        if (getEnvSafe("LOCALAPPDATA", tmp, sizeof(tmp)))
            snprintf(localAppData, sizeof(localAppData), "%s\\", tmp);
    }

    char roamingAppData[MAX_PATH] = {};
    {
        char tmp[MAX_PATH];
        if (getEnvSafe("APPDATA", tmp, sizeof(tmp)))
            snprintf(roamingAppData, sizeof(roamingAppData), "%s\\", tmp);
    }

    char myDocuments[MAX_PATH] = {};
    {
        std::string known = GetKnownFolderPathString(FOLDERID_Documents);
        if (!known.empty()) {
            snprintf(myDocuments, sizeof(myDocuments), "%s\\", known.c_str());
        } else {
            char tmp[MAX_PATH];
            if (getEnvSafe("USERPROFILE", tmp, sizeof(tmp)))
                snprintf(myDocuments, sizeof(myDocuments), "%s\\Documents\\", tmp);
        }
    }

    char savedGames[MAX_PATH] = {};
    {
        std::string known = GetKnownFolderPathString(FOLDERID_SavedGames);
        if (!known.empty()) {
            snprintf(savedGames, sizeof(savedGames), "%s\\", known.c_str());
        } else {
            char tmp[MAX_PATH];
            if (getEnvSafe("USERPROFILE", tmp, sizeof(tmp)))
                snprintf(savedGames, sizeof(savedGames), "%s\\Saved Games\\", tmp);
        }
    }

    std::string gameInstallPath = FindGameInstallPath(steamPath, appId);

    // rootId values from Steam's ERemoteStorageFileRoot enum (confirmed via IDA):
    //   0=Default, 1=GameInstall, 2=WinMyDocuments, 3=WinAppDataLocal,
    //   4=WinAppDataRoaming, 6=WinSavedGames, 12=WinAppDataLocalLow
    RootMapping mappings[] = {
        {"",                   "",                    0, (appUserdataDir / "remote").string()},
        {"GameInstall",        "%GameInstall%",        1, gameInstallPath},
        {"WinAppDataLocalLow", "%WinAppDataLocalLow%", 12, std::string(localLow)},
        {"WinAppDataLocal",    "%WinAppDataLocal%",    3, std::string(localAppData)},
        {"WinAppDataRoaming",  "%WinAppDataRoaming%",  4, std::string(roamingAppData)},
        {"WinMyDocuments",     "%WinMyDocuments%",     2, std::string(myDocuments)},
        {"WinSavedGames",      "%WinSavedGames%",      6, std::string(savedGames)},
    };

    std::unordered_map<std::string, std::string> seenRootsByCloudPath;
    bool hasRootCollision = false;
    bool scanLimitHit = false;
    size_t visitedFiles = 0;
    auto scanStart = std::chrono::steady_clock::now();
    auto scanLimitReached = [&]() {
        auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - scanStart).count();
        if (visitedFiles >= kMaxAutoCloudScanFiles || elapsedMs >= kMaxAutoCloudScanMillis) {
            scanLimitHit = true;
            LOG("GetAutoCloudFileList: stopping app %u scan after %zu files and %lld ms",
                appId, visitedFiles, (long long)elapsedMs);
            return true;
        }
        return false;
    };
    for (const auto& rule : rules) {
        if (!AutoCloudRuleMatchesCurrentPlatform(rule.platforms)) {
            LOG("GetAutoCloudFileList: skipping app %u rule root='%s' path='%s' (platforms mask=0x%x excludes Windows)",
                appId, rule.root.c_str(), rule.path.c_str(), rule.platforms);
            continue;
        }
        const RootMapping* mapping = nullptr;
        std::string ruleRootLower = ToLowerAscii(rule.root);
        for (const auto& candidate : mappings) {
            if (ToLowerAscii(candidate.dirName) == ruleRootLower) {
                mapping = &candidate;
                break;
            }
        }

        if (!mapping) {
            LOG("GetAutoCloudFileList: skipping app %u rule with unknown root '%s'", appId, rule.root.c_str());
            continue;
        }
        if (mapping->envExpansion.empty()) {
            LOG("GetAutoCloudFileList: skipping app %u rule root '%s' because filesystem root is unresolved",
                appId, rule.root.c_str());
            continue;
        }

        std::string normalizedCloudPath = ExpandAutoCloudPathTokens(NormalizeSlashes(rule.path), accountId);
        std::string normalizedScanPath = ExpandAutoCloudPathTokens(NormalizeSlashes(rule.resolvedPath), accountId);
        if (normalizedCloudPath == ".") normalizedCloudPath.clear();
        if (normalizedScanPath == ".") normalizedScanPath.clear();
        if (!IsSafeRelativePath(normalizedCloudPath) || !IsSafeRelativePath(normalizedScanPath)) {
            LOG("GetAutoCloudFileList: skipping unsafe app %u rule path '%s'", appId, rule.path.c_str());
            continue;
        }
        while (!normalizedCloudPath.empty() && normalizedCloudPath.front() == '/') normalizedCloudPath.erase(0, 1);
        while (!normalizedCloudPath.empty() && normalizedCloudPath.back() == '/') normalizedCloudPath.pop_back();
        while (!normalizedScanPath.empty() && normalizedScanPath.front() == '/') normalizedScanPath.erase(0, 1);
        while (!normalizedScanPath.empty() && normalizedScanPath.back() == '/') normalizedScanPath.pop_back();

        std::filesystem::path scanRoot = std::filesystem::path(mapping->envExpansion);
        if (!normalizedScanPath.empty()) {
            std::filesystem::path rel;
            std::stringstream ss(normalizedScanPath);
            std::string part;
            while (std::getline(ss, part, '/')) {
                if (!part.empty()) rel /= part;
            }
            scanRoot /= rel;
        }

        if (!std::filesystem::exists(scanRoot) || !std::filesystem::is_directory(scanRoot)) {
            LOG("GetAutoCloudFileList: app %u rule path missing: root='%s' path='%s' resolved='%s'",
                appId, rule.root.c_str(), rule.path.c_str(), scanRoot.string().c_str());
            continue;
        }

        LOG("GetAutoCloudFileList: app %u rule root='%s' path='%s' resolvedPath='%s' pattern='%s' recursive=%u resolved='%s'",
            appId, rule.root.c_str(), rule.path.c_str(), rule.resolvedPath.c_str(),
            rule.pattern.c_str(), rule.recursive ? 1 : 0, scanRoot.string().c_str());

        auto considerFile = [&](const std::filesystem::directory_entry& entry) {
            std::error_code fileEc;
            if (!entry.is_regular_file(fileEc)) return;
            ++visitedFiles;
            std::string relFromRoot = NormalizeSlashes(std::filesystem::relative(entry.path(), scanRoot, fileEc).string());
            if (fileEc) return;
            std::string leaf = entry.path().filename().string();
            if (leaf == "steam_autocloud.vdf") return;
            std::string pattern = NormalizeSlashes(rule.pattern.empty() ? "*" : rule.pattern);
            const std::string& matchTarget = pattern.find('/') == std::string::npos ? leaf : relFromRoot;
            if (!WildcardMatchInsensitive(pattern, matchTarget)) return;

            for (const auto& excludePattern : rule.excludes) {
                std::string exPat = NormalizeSlashes(excludePattern);
                const std::string& exTarget = exPat.find('/') == std::string::npos ? leaf : relFromRoot;
                if (WildcardMatchInsensitive(exPat, exTarget)) return;
            }

            std::string cloudPath = normalizedCloudPath.empty() ? relFromRoot : normalizedCloudPath + "/" + relFromRoot;
            std::string collisionKey = ToLowerAscii(NormalizeSlashes(cloudPath));
            auto seenIt = seenRootsByCloudPath.find(collisionKey);
            if (seenIt != seenRootsByCloudPath.end()) {
                if (seenIt->second != mapping->rootToken) {
                    LOG("GetAutoCloudFileList: root collision for app %u cloud path %s (%s vs %s); aborting bootstrap",
                        appId, cloudPath.c_str(), seenIt->second.c_str(), mapping->rootToken.c_str());
                    hasRootCollision = true;
                }
                return;
            }
            seenRootsByCloudPath[collisionKey] = mapping->rootToken;
            addFile(entry, cloudPath, entry.path().string(), mapping->rootToken, mapping->rootId);
        };

        if (rule.recursive) {
            std::error_code iterEc;
            std::filesystem::recursive_directory_iterator it(
                scanRoot, std::filesystem::directory_options::skip_permission_denied, iterEc);
            std::filesystem::recursive_directory_iterator end;
            for (; !iterEc && it != end; it.increment(iterEc)) {
                if (scanLimitReached()) break;
                considerFile(*it);
            }
        } else {
            std::error_code iterEc;
            std::filesystem::directory_iterator it(
                scanRoot, std::filesystem::directory_options::skip_permission_denied, iterEc);
            std::filesystem::directory_iterator end;
            for (; !iterEc && it != end; it.increment(iterEc)) {
                if (scanLimitReached()) break;
                considerFile(*it);
            }
        }
        if (scanLimitReached()) break;
    }

    if (scanLimitHit) {
        throw std::runtime_error("AutoCloud scan limit reached before complete enumeration");
    }

    if (hasRootCollision) {
        result.clear();
        LOG("GetAutoCloudFileList: aborting app %u bootstrap due to root/path collision", appId);
    }

    LOG("GetAutoCloudFileList: found %zu rule-matched Auto-Cloud files for app %u", result.size(), appId);
    for (auto& fe : result) {
        LOG("  AC file: root=%u %s (%llu bytes)", fe.rootId, fe.filename.c_str(), fe.rawSize);
    }
    return result;
}

void SaveRootTokens(uint32_t accountId, uint32_t appId, const std::unordered_set<std::string>& tokens) {
    std::lock_guard<std::shared_mutex> lock(g_mutex);
    std::string appDir = GetAppPathInternal(accountId, appId);
    std::filesystem::create_directories(appDir);
    std::string path = appDir + "root_token.dat";
    std::string content;
    for (auto& t : tokens) {
        content += t + "\n";
    }
    if (FileUtil::AtomicWriteText(path, content)) {
        LOG("SaveRootTokens: persisted %zu tokens for app %u", tokens.size(), appId);
    } else {
        LOG("SaveRootTokens: failed for app %u", appId);
    }
}

std::unordered_set<std::string> LoadRootTokens(uint32_t accountId, uint32_t appId) {
    std::unordered_set<std::string> tokens;
    bool needsRewrite = false;

    // Read phase: shared lock allows concurrent readers
    {
        std::shared_lock<std::shared_mutex> lock(g_mutex);
        std::string path = GetAppPathInternal(accountId, appId) + "root_token.dat";
        std::ifstream f(path);
        if (f) {
            std::string line;
            while (std::getline(f, line)) {
                std::string original = line;
                // Strip trailing \r (Windows CRLF line endings or corrupted data)
                while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
                    line.pop_back();
                if (line != original)
                    needsRewrite = true;
                if (!line.empty()) {
                    tokens.insert(line);
                }
            }
            f.close();
            if (!tokens.empty()) {
                LOG("LoadRootTokens: loaded %zu tokens from disk for app %u", tokens.size(), appId);
            }
        }
    }

    // Rewrite phase: upgrade to exclusive lock via SaveRootTokens if cleanup needed
    if (needsRewrite && !tokens.empty()) {
        LOG("LoadRootTokens: cleaning corrupted tokens for app %u", appId);
        SaveRootTokens(accountId, appId, tokens);
    }

    return tokens;
}

void SaveFileTokens(uint32_t accountId, uint32_t appId,
                    const std::unordered_map<std::string, std::string>& fileTokens) {
    std::lock_guard<std::shared_mutex> lock(g_mutex);
    std::string appDir = GetAppPathInternal(accountId, appId);
    std::filesystem::create_directories(appDir);
    std::string path = appDir + "file_tokens.dat";
    std::string content;
    for (auto& [cleanName, token] : fileTokens) {
        content += cleanName + "\t" + token + "\n";
    }
    if (FileUtil::AtomicWriteText(path, content)) {
        LOG("SaveFileTokens: persisted %zu entries for app %u", fileTokens.size(), appId);
    } else {
        LOG("SaveFileTokens: failed for app %u", appId);
    }
}

std::unordered_map<std::string, std::string> LoadFileTokens(uint32_t accountId, uint32_t appId) {
    std::shared_lock<std::shared_mutex> lock(g_mutex);
    std::string path = GetAppPathInternal(accountId, appId) + "file_tokens.dat";
    std::unordered_map<std::string, std::string> result;
    std::ifstream f(path);
    if (f) {
        std::string line;
        while (std::getline(f, line)) {
            // Strip trailing \r (CRLF)
            while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
                line.pop_back();
            if (line.empty()) continue;
            auto tab = line.find('\t');
            if (tab == std::string::npos) continue;
            std::string cleanName = line.substr(0, tab);
            std::string token = line.substr(tab + 1);
            if (!cleanName.empty()) {
                result[cleanName] = token;
            }
        }
        if (!result.empty()) {
            LOG("LoadFileTokens: loaded %zu entries from disk for app %u", result.size(), appId);
        }
    }
    return result;
}

// ---- Delete tombstones ---------------------------------------------------
//
// Persisted as "deleted.dat" under the app dir, one entry per line as
// "filename\tcn\n". The file is treated as LocalStorage-internal metadata:
// it is not enumerated by GetFileList, not pruned by RemoveLocalBlobsNotInCloud,
// and not seeded by the provider-gap repair path. It is deliberately NOT
// mirrored to cloud; keeping tombstones strictly local avoids cross-client
// contention between a slow delete and a fresh save of the same filename on
// another machine.
//
// The CN is the local change number at the moment of MarkDeleted. SyncFromCloud
// compares it against the cloud CN to decide whether a tombstoned entry should
// still suppress download: if cloud's CN is strictly newer than the tombstone
// CN, a different machine wrote after our delete, and Steam's "newer wins"
// semantics prevail — the tombstone is bypassed.
//
// Legacy lines without a tab are loaded with cn=0 (suppressing any nonzero
// cloud CN never arises in practice since CN starts at 2 — so a cn=0 tombstone
// behaves identically to the pre-CN behavior of "always suppress").

static std::unordered_map<std::string, uint64_t> LoadDeletedLocked(uint32_t accountId,
                                                                   uint32_t appId,
                                                                   bool* outNeedsRewrite = nullptr) {
    // Caller holds g_mutex (shared or exclusive).
    std::unordered_map<std::string, uint64_t> deleted;
    if (outNeedsRewrite) *outNeedsRewrite = false;
    std::string path = GetAppPathInternal(accountId, appId) + "deleted.dat";
    std::ifstream f(path);
    if (!f) return deleted;
    std::string line;
    while (std::getline(f, line)) {
        std::string original = line;
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
            line.pop_back();
        if (line != original && outNeedsRewrite) *outNeedsRewrite = true;
        if (line.empty()) continue;
        auto tab = line.find('\t');
        std::string fname;
        uint64_t cn = 0;
        if (tab == std::string::npos) {
            fname = line;
            if (outNeedsRewrite) *outNeedsRewrite = true; // legacy row, upgrade on rewrite
        } else {
            fname = line.substr(0, tab);
            std::string cnStr = line.substr(tab + 1);
            // Parse unsigned-decimal only; reject sign, whitespace, overflow.
            bool ok = !cnStr.empty();
            for (char c : cnStr) {
                if (c < '0' || c > '9') { ok = false; break; }
            }
            if (ok) {
                try {
                    unsigned long long v = std::stoull(cnStr);
                    cn = static_cast<uint64_t>(v);
                } catch (...) { ok = false; }
            }
            if (!ok && outNeedsRewrite) *outNeedsRewrite = true;
        }
        if (fname.empty()) continue;
        // Keep the highest CN for a filename if duplicates exist on disk.
        auto it = deleted.find(fname);
        if (it == deleted.end() || it->second < cn) {
            deleted[fname] = cn;
        }
    }
    return deleted;
}

static bool SaveDeletedLocked(uint32_t accountId, uint32_t appId,
                              const std::unordered_map<std::string, uint64_t>& deleted) {
    // Caller holds g_mutex exclusively. Returns true on successful persistence.
    std::string appDir = GetAppPathInternal(accountId, appId);
    std::error_code mkEc;
    std::filesystem::create_directories(appDir, mkEc);
    std::string path = appDir + "deleted.dat";
    if (deleted.empty()) {
        std::error_code ec;
        std::filesystem::remove(path, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            LOG("SaveDeletedLocked: failed to remove empty tombstone file for app %u: %s",
                appId, ec.message().c_str());
            return false;
        }
        return true;
    }
    std::string content;
    for (auto& kv : deleted) {
        content += kv.first;
        content += '\t';
        content += std::to_string(kv.second);
        content += '\n';
    }
    if (!FileUtil::AtomicWriteText(path, content)) {
        LOG("SaveDeletedLocked: FAILED to persist %zu tombstone(s) for app %u — "
            "deletion may resurrect on next SyncFromCloud", deleted.size(), appId);
        return false;
    }
    return true;
}

std::unordered_map<std::string, uint64_t> LoadDeleted(uint32_t accountId, uint32_t appId) {
    bool needsRewrite = false;
    std::unordered_map<std::string, uint64_t> deleted;
    {
        std::shared_lock<std::shared_mutex> lock(g_mutex);
        deleted = LoadDeletedLocked(accountId, appId, &needsRewrite);
    }
    // Upgrade legacy/dirty rows to canonical form under an exclusive lock, but
    // only when there's something worth rewriting. Mirrors LoadRootTokens.
    //
    // The authoritative state is whatever is on disk right now: a concurrent
    // ClearDeleted between our shared read and this exclusive acquire may
    // have legitimately removed entries we saw, and we MUST NOT resurrect
    // those. Conversely any concurrent MarkDeleted holds the exclusive lock
    // and its write is durable before we reach here, so the fresh snapshot
    // already reflects it. We therefore rewrite `latest` verbatim for the
    // legacy-format upgrade without merging in the stale shared-lock view.
    if (needsRewrite) {
        std::lock_guard<std::shared_mutex> lock(g_mutex);
        bool latestNeedsRewrite = false;
        auto latest = LoadDeletedLocked(accountId, appId, &latestNeedsRewrite);
        if (latestNeedsRewrite && !latest.empty()) {
            SaveDeletedLocked(accountId, appId, latest);
        }
        deleted = std::move(latest);
    }
    return deleted;
}

void MarkDeleted(uint32_t accountId, uint32_t appId, const std::string& filename,
                 uint64_t cnAtDelete) {
    if (filename.empty()) return;
    std::lock_guard<std::shared_mutex> lock(g_mutex);
    auto deleted = LoadDeletedLocked(accountId, appId, nullptr);
    auto it = deleted.find(filename);
    bool inserted = (it == deleted.end());
    if (!inserted && it->second >= cnAtDelete) {
        // Already tombstoned at equal-or-higher CN; no-op avoids redundant I/O.
        return;
    }
    deleted[filename] = cnAtDelete;
    if (!SaveDeletedLocked(accountId, appId, deleted)) {
        // Persistence failed — the in-memory snapshot is now ahead of disk.
        // The next LoadDeletedLocked will reflect only what's on disk, so
        // functionally we've rolled back. Caller is already informed via LOG
        // from SaveDeletedLocked; just log the site context.
        LOG("MarkDeleted: app %u tombstone for %s (cn=%llu) NOT persisted",
            appId, filename.c_str(), (unsigned long long)cnAtDelete);
        return;
    }
    LOG("MarkDeleted: app %u tombstoned %s at cn=%llu (%zu total)",
        appId, filename.c_str(), (unsigned long long)cnAtDelete, deleted.size());
}

void ClearDeleted(uint32_t accountId, uint32_t appId, const std::string& filename) {
    if (filename.empty()) return;
    std::lock_guard<std::shared_mutex> lock(g_mutex);
    auto deleted = LoadDeletedLocked(accountId, appId, nullptr);
    if (deleted.erase(filename) > 0) {
        if (!SaveDeletedLocked(accountId, appId, deleted)) {
            LOG("ClearDeleted: app %u clear for %s NOT persisted", appId, filename.c_str());
            return;
        }
        LOG("ClearDeleted: app %u cleared %s (%zu remaining)",
            appId, filename.c_str(), deleted.size());
    }
}

bool IsDeleted(uint32_t accountId, uint32_t appId, const std::string& filename) {
    if (filename.empty()) return false;
    std::shared_lock<std::shared_mutex> lock(g_mutex);
    // Single-name membership: stream the file line-by-line instead of building
    // the full map, so a future hot-path caller pays O(lines) I/O without also
    // paying O(lines) hash-set allocation.
    std::string path = GetAppPathInternal(accountId, appId) + "deleted.dat";
    std::ifstream f(path);
    if (!f) return false;
    std::string line;
    while (std::getline(f, line)) {
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n'))
            line.pop_back();
        if (line.empty()) continue;
        auto tab = line.find('\t');
        std::string fname = (tab == std::string::npos) ? line : line.substr(0, tab);
        if (fname == filename) return true;
    }
    return false;
}

} // namespace LocalStorage
