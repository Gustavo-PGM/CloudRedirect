#include "log.h"
#include <cstdarg>
#include <ctime>

namespace Log {

static FILE* g_file = nullptr;
static std::mutex g_mutex;
static std::string g_logPath;

static constexpr long MAX_LOG_SIZE = 10 * 1024 * 1024;

// Check file size and rotate if needed (caller must hold g_mutex)
static void RotateIfNeeded() {
    if (!g_file) return;
    long pos = ftell(g_file);
    if (pos < 0 || pos < MAX_LOG_SIZE) return;

    fclose(g_file);

    // Rename current log to .old (overwrite any previous .old)
    std::string oldPath = g_logPath + ".old";
    remove(oldPath.c_str());
    rename(g_logPath.c_str(), oldPath.c_str());

    g_file = fopen(g_logPath.c_str(), "a");
    if (g_file) {
        fprintf(g_file, "=== Log rotated (previous log saved as .old) ===\n");
        fflush(g_file);
    }
}

void Init(const char* path) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_logPath = path;
    g_file = fopen(path, "a");
    if (g_file) {
        time_t t = time(nullptr);
        tm lt;
        localtime_s(&lt, &t);
        char buf[64];
        strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &lt);
        fprintf(g_file, "\n=== CloudRedirect loaded at %s ===\n", buf);
        fflush(g_file);
    }
}

void Shutdown() {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (g_file) {
        fprintf(g_file, "=== CloudRedirect unloaded ===\n");
        fclose(g_file);
        g_file = nullptr;
    }
}

void Write(const char* fmt, ...) {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (!g_file) return;

    RotateIfNeeded();
    if (!g_file) return; // rotation may have failed

    time_t t = time(nullptr);
    tm lt;
    localtime_s(&lt, &t);
    char ts[32];
    strftime(ts, sizeof(ts), "%H:%M:%S", &lt);
    fprintf(g_file, "[%s] ", ts);

    va_list args;
    va_start(args, fmt);
    vfprintf(g_file, fmt, args);
    va_end(args);

    fprintf(g_file, "\n");
    fflush(g_file);
}

} // namespace Log
