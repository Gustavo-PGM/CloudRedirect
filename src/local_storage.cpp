#include "local_storage.h"
#include "file_util.h"
#include "log.h"
#include <wincrypt.h>
#include <algorithm>
#include <shared_mutex>
#include <sstream>
#pragma comment(lib, "Advapi32.lib")

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
            if (relPath == "cn.dat" || relPath == "root_token.dat" || relPath == "file_tokens.dat") continue;

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


std::vector<FileEntry> GetAutoCloudFileList(const std::string& steamPath, uint32_t appId) {
    std::vector<FileEntry> result;

    std::string basePath = steamPath;
    if (!basePath.empty() && basePath.back() != '\\')
        basePath += '\\';

    std::string userdataRoot = basePath + "userdata";

    if (!std::filesystem::exists(userdataRoot) || !std::filesystem::is_directory(userdataRoot)) {
        LOG("GetAutoCloudFileList: userdata dir not found: %s", userdataRoot.c_str());
        return result;
    }

    std::string appIdStr = std::to_string(appId);
    std::string acDir;
    std::error_code dirEc;
    for (auto& acctEntry : std::filesystem::directory_iterator(userdataRoot, dirEc)) {
        if (!acctEntry.is_directory()) continue;
        auto candidate = acctEntry.path() / appIdStr / "ac";
        if (std::filesystem::exists(candidate) && std::filesystem::is_directory(candidate)) {
            acDir = candidate.string();
            break;
        }
    }
    if (acDir.empty()) {
        LOG("GetAutoCloudFileList: no ac/ dir found for app %u", appId);
        return result;
    }

    struct RootMapping {
        std::string dirName;
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
        char tmp[MAX_PATH];
        if (getEnvSafe("USERPROFILE", tmp, sizeof(tmp)))
            snprintf(myDocuments, sizeof(myDocuments), "%s\\Documents\\", tmp);
    }

    // resolve GameInstall path from appmanifest_<appId>.acf -> installdir
    std::string gameInstallPath;
    {
        char manifestPath[1024];
        snprintf(manifestPath, sizeof(manifestPath), "%ssteamapps\\appmanifest_%u.acf",
                 basePath.c_str(), appId);
        LOG("GetAutoCloudFileList: reading manifest: %s", manifestPath);
        std::ifstream mf(manifestPath);
        if (mf) {
            std::string line;
            while (std::getline(mf, line)) {
                auto pos = line.find("\"installdir\"");
                if (pos == std::string::npos) continue;
                auto q1 = line.rfind('"');
                auto q2 = line.rfind('"', q1 - 1);
                if (q1 != std::string::npos && q2 != std::string::npos && q1 > q2) {
                    std::string installDir = line.substr(q2 + 1, q1 - q2 - 1);
                    // basePath already ends with backslash
                    gameInstallPath = basePath + "steamapps\\common\\" + installDir + "\\";
                }
                break;
            }
        } else {
            LOG("GetAutoCloudFileList: FAILED to open manifest: %s", manifestPath);
        }
        if (!gameInstallPath.empty())
            LOG("GetAutoCloudFileList: GameInstall path: %s", gameInstallPath.c_str());
        else
            LOG("GetAutoCloudFileList: GameInstall path is EMPTY (manifest parse failed?)");
    }

    // rootId values from Steam's ERemoteStorageFileRoot enum (confirmed via IDA):
    //   0=Default, 1=GameInstall, 2=WinMyDocuments, 3=WinAppDataLocal,
    //   4=WinAppDataRoaming, 12=WinAppDataLocalLow
    RootMapping mappings[] = {
        {"GameInstall",        1, gameInstallPath},
        {"WinAppDataLocalLow", 12, std::string(localLow)},
        {"WinAppDataLocal",    3, std::string(localAppData)},
        {"WinAppDataRoaming",  4, std::string(roamingAppData)},
        {"WinMyDocuments",     2, std::string(myDocuments)},
    };

    for (auto& mapping : mappings) {
        auto rootSubdir = std::filesystem::path(acDir) / mapping.dirName;
        if (!std::filesystem::exists(rootSubdir)) continue;

        std::string acRootStr = rootSubdir.string();
        LOG("GetAutoCloudFileList: scanning AC root: %s (envExpansion=%s)",
            acRootStr.c_str(), mapping.envExpansion.c_str());

        if (mapping.envExpansion.empty()) {
            LOG("GetAutoCloudFileList: SKIPPING %s — envExpansion is empty", mapping.dirName.c_str());
            continue;
        }

        // Helper lambda: scan a real directory for files and add to result.
        // relPrefix is the cloud path prefix (empty for root, "subdir" for subdirs).
        auto scanDir = [&](const std::string& realDir, const std::string& relPrefix) {
            if (!std::filesystem::exists(realDir) || !std::filesystem::is_directory(realDir))
                return;
            for (auto& fileEntry : std::filesystem::directory_iterator(realDir)) {
                if (!fileEntry.is_regular_file()) continue;

                std::string fileName = fileEntry.path().filename().string();
                if (fileName == "steam_autocloud.vdf") continue;

                std::string cloudPath = relPrefix.empty() ? fileName : relPrefix + "/" + fileName;

                auto sha = SHA1File(fileEntry.path().string());
                auto ftime = std::filesystem::last_write_time(fileEntry.path());
                uint64_t ts = FileTimeToUnixSeconds(ftime);

                FileEntry fe;
                fe.filename = cloudPath;
                fe.sha = sha;
                fe.timestamp = ts;
                fe.rawSize = (uint64_t)fileEntry.file_size();
                fe.deleted = false;
                fe.rootId = mapping.rootId;
                result.push_back(std::move(fe));
            }
        };

        // Scan root-level files first (files directly at mapping.envExpansion)
        scanDir(mapping.envExpansion, "");

        // Then scan subdirectories found in the AC tree
        for (auto& acEntry : std::filesystem::recursive_directory_iterator(acRootStr)) {
            if (!acEntry.is_directory()) continue;
            std::string relDir = std::filesystem::relative(acEntry.path(), acRootStr).string();
            for (auto& c : relDir) { if (c == '\\') c = '/'; }

            std::string realDir = mapping.envExpansion + relDir;
            for (auto& c : realDir) { if (c == '/') c = '\\'; }

            scanDir(realDir, relDir);
        }
    }

    LOG("GetAutoCloudFileList: found %zu Auto-Cloud files for app %u", result.size(), appId);
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
            if (!cleanName.empty() && !token.empty()) {
                result[cleanName] = token;
            }
        }
        if (!result.empty()) {
            LOG("LoadFileTokens: loaded %zu entries from disk for app %u", result.size(), appId);
        }
    }
    return result;
}

} // namespace LocalStorage
