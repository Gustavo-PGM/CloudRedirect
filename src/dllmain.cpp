#include "common.h"
#include "log.h"
#include "cloud_intercept.h"
#include "file_util.h"
#include <mutex>

static HMODULE g_thisModule = nullptr;
static std::once_flag g_initFlag;

// Get Steam directory from the DLL's own location, as a UTF-8 string.
//
// Important: the entire DLL treats every "narrow" std::string path as UTF-8 —
// FileUtil::Utf8ToPath / PathToUtf8, all ifstream/ofstream opens, all
// filesystem::path construction downstream. This function is the single
// root of g_steamPath; if it returned an ACP-encoded path (as a naive
// GetModuleFileNameA would), every non-ASCII Steam install path
// (e.g. "C:\Users\Владимир\Steam\", "D:\游戏\Steam\") would silently
// corrupt downstream: save files would be written to wrong-byte paths,
// reads would miss, libraryfolders.vdf would not be found, and
// auto-cloud would misidentify the install as missing.
//
// GetModuleFileNameW returns wchar_t, which we explicitly convert via
// WideCharToMultiByte(CP_UTF8) — the same path every other
// wide-to-narrow call in the codebase uses (see
// GetKnownFolderPathString at src/local_storage.cpp:132).
static std::string GetSteamPath() {
    wchar_t wdllPath[MAX_PATH];
    DWORD n = GetModuleFileNameW(g_thisModule, wdllPath, MAX_PATH);
    if (n == 0 || n >= MAX_PATH) return {};

    // Trim to parent directory (everything up to and including the final '\').
    // We do the trim on wide data before UTF-8 encode so we never split a
    // multi-byte sequence. Fallback to full path if no separator found.
    DWORD endIdx = n;
    for (DWORD i = n; i > 0; --i) {
        if (wdllPath[i - 1] == L'\\') { endIdx = i; break; }
    }

    // Route through FileUtil::WideToUtf8 — the codebase's single canonical
    // wide→UTF-8 encoder. The handcrafted block this replaces did not pass
    // WC_ERR_INVALID_CHARS, so a malformed UTF-16 module path (lone surrogate
    // injected by an attacker who managed to load the DLL from a hand-built
    // junk path) would silently produce junk UTF-8 here. The helper rejects
    // ill-formed input and returns empty, which the call_once init below
    // tolerates (logs init failure and skips hooks, vs. corrupting the log).
    return FileUtil::WideToUtf8(wdllPath, (size_t)endIdx);
}

// exported function called by the SteamTools payload code cave
// signature: int CloudOnSendPkt(void* thisptr, const uint8_t* data, uint32_t size, void* recvPktFn)
// returns non-zero if packet was handled (caller should skip original SendPkt handler)
// returns zero to let the original SteamTools SendPkt handler process it
extern "C" __declspec(dllexport)
int CloudOnSendPkt(void* thisptr, const uint8_t* data, uint32_t size, void* recvPktFn) {
    // one-time init on first call (we're inside the Steam process by now)
    // If call_once throws, the flag remains unset and the next call retries.
    // We catch internally to prevent partial initialization from leaving the system
    // in an inconsistent state — if init fails, we log and mark as failed so
    // subsequent calls skip init and return 0 (let Steam handle the packet).
    static bool g_initFailed = false;
    std::call_once(g_initFlag, [&]() {
        try {
            std::string steamPath = GetSteamPath();
            std::string logPath = steamPath + "cloud_redirect.log";

            Log::Init(logPath.c_str());
            LOG("CloudRedirect loaded via code cave, PID=%u", GetCurrentProcessId());
            LOG("Steam path: %s", steamPath.c_str());

            // Log module bases for mapping runtime addresses to IDA
            HMODULE hSteamClient = GetModuleHandleA("steamclient64.dll");
            LOG("steamclient64.dll base: %p", hSteamClient);

            CloudIntercept::Init(steamPath);

            if (recvPktFn) {
                CloudIntercept::SetSendPktAddr(recvPktFn);

                // install RecvPkt monitor hook for response interception
                // recvPktFn points to the RecvPkt vtable slot (RVA 0x1CAB48)
                // the saved original RecvPkt is at RVA 0x1CAB20
                uintptr_t recvPktGlobal = (uintptr_t)recvPktFn;
                uintptr_t payloadBase = recvPktGlobal - 0x1CAB48;
                uintptr_t savedOrigAddr = payloadBase + 0x1CAB20;
                CloudIntercept::InstallRecvPktMonitor((void*)savedOrigAddr);
            }

            // install inline detour on steamclient64 for manifest pinning
            CloudIntercept::InstallManifestPinHook();

            LOG("CloudRedirect fully initialized with hooks");
        } catch (const std::exception& ex) {
            LOG("CloudRedirect init FAILED: %s", ex.what());
            g_initFailed = true;
        } catch (...) {
            LOG("CloudRedirect init FAILED: unknown exception");
            g_initFailed = true;
        }
    });

    if (g_initFailed) return 0;

    // delegate to intercept handler
    return CloudIntercept::OnSendPkt(thisptr, data, size) ? 1 : 0;
}

BOOL APIENTRY DllMain(HMODULE hModule, DWORD reason, LPVOID reserved) {
    switch (reason) {
    case DLL_PROCESS_ATTACH:
        g_thisModule = hModule;
        DisableThreadLibraryCalls(hModule);

        // Pin the DLL so FreeLibrary can never unload it.
        // This prevents crashes from threads still executing hook code
        // after the loader tears us down.
        {
            HMODULE pinned = nullptr;
            GetModuleHandleExA(
                GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_PIN,
                reinterpret_cast<LPCSTR>(&DllMain),
                &pinned);
        }
        break;

    case DLL_PROCESS_DETACH:
        // reserved != NULL means process is terminating (ExitProcess).
        // Other threads are already dead — joining them or acquiring mutexes
        // would deadlock. Only run cleanup on explicit FreeLibrary (reserved == NULL).
        if (reserved == nullptr) {
            CloudIntercept::Shutdown();
            Log::Shutdown();
        }
        break;
    }
    return TRUE;
}
