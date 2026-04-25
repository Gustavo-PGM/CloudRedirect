#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <memory>

// ICloudProvider -- abstract interface for cloud storage backends.
// All paths are relative with forward slashes: "{accountId}/{appId}/blobs/{filename}"
// Implementations: GoogleDriveProvider, OneDriveProvider, LocalDiskProvider.

class ICloudProvider {
public:
    enum class ExistsStatus { Missing, Exists, Error };

    virtual ~ICloudProvider() = default;

    // Human-readable name ("Google Drive", "OneDrive", "Local Disk").
    virtual const char* Name() const = 0;

    // Initialize the provider with a config/credentials path.
    // For GDrive/OneDrive: path to tokens.json.
    // For LocalDisk: path to the storage root directory.
    virtual bool Init(const std::string& configPath) = 0;

    // Shut down gracefully -- drain pending operations, release resources.
    virtual void Shutdown() = 0;

    // True if the provider has valid credentials (or doesn't need them).
    virtual bool IsAuthenticated() const = 0;

    // Blocking file operations

    // Upload a file. Creates parent directories/folders as needed.
    // Overwrites if the file already exists.
    virtual bool Upload(const std::string& path,
                        const uint8_t* data, size_t len) = 0;

    // Download a file into outData. Returns false if the file doesn't exist
    // or the download fails.
    virtual bool Download(const std::string& path,
                          std::vector<uint8_t>& outData) = 0;

    // Delete a file. Returns true if deleted or already absent.
    virtual bool Remove(const std::string& path) = 0;

    // Check if a file exists.
    virtual bool Exists(const std::string& path) = 0;

    // Check if a file exists, preserving provider/API errors when available.
    virtual ExistsStatus CheckExists(const std::string& path) = 0;

    // Listing

    struct FileInfo {
        std::string path;         // relative path (same format as other calls)
        uint64_t    size = 0;     // file size in bytes
        uint64_t    modifiedTime = 0; // Unix timestamp (seconds)
    };

    // List all files under a prefix (e.g., "54303850/1229490/blobs/").
    // Returns empty vector if the prefix doesn't exist.
    virtual std::vector<FileInfo> List(const std::string& prefix) = 0;

    // Same as List(), but returns false when the provider could not verify the
    // listing due to an API/filesystem error. A missing prefix is a successful
    // empty listing.
    //
    // When `outComplete` is non-null, the provider writes `true` iff the
    // returned listing is known-complete. Providers that hit a recursion cap,
    // tolerated pagination failure, or silently-skipped entries must write
    // `false` even when the function returns `true`. Callers use this flag
    // to refuse destructive operations (e.g. pruning local blobs that
    // "aren't in cloud") on listings they can't fully trust.
    //
    // Convention: overrides MUST initialize `*outComplete = false` at entry
    // and write `true` only on a code path that has verified full
    // enumeration. A forgotten early-return therefore leaves the listing
    // marked incomplete (safe default: caller skips prune) rather than
    // falsely complete (unsafe: caller may delete real data).
    virtual bool ListChecked(const std::string& prefix, std::vector<FileInfo>& outFiles,
                             bool* outComplete = nullptr) {
        if (outComplete) *outComplete = false;
        outFiles = List(prefix);
        if (outComplete) *outComplete = true;
        return true;
    }
};

// Factory: create a provider by name.
// Recognized names: "gdrive", "onedrive", "local" (case-insensitive).
// Returns nullptr for unknown names.
std::unique_ptr<ICloudProvider> CreateCloudProvider(const std::string& name);
