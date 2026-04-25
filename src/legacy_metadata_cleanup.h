#pragma once
#include <string>
#include <vector>
#include <cstdint>

// Legacy metadata cleanup.
//
// Before canonicalization landed, the DLL wrote Steam-facing internal metadata
// (`Playtime.bin`, `UserGameStats.bin`) using their raw filenames into places
// they should never have lived:
//   - directly under `Steam\userdata\{acct}\{app}\remote\` (Steam's cloud save
//     view -- which makes the UI contamination scanner flag them as "saves"),
//   - as duplicated top-level entries inside our private blob cache
//     `cloud_redirect\storage\{acct}\{app}\` alongside the newer
//     `.cloudredirect\{same}` canonical path,
//   - and with raw filenames in the cloud provider under `{acct}/{app}/blobs/`
//     paired with the canonical `.cloudredirect/{same}` blob.
//
// The current DLL never reads or writes any of those legacy paths, but leftover
// artifacts from older installs stick around forever because nothing cleans
// them up. These three helpers do the sweep.
//
// All helpers are idempotent: absent inputs produce zero work, present inputs
// produce the same end state regardless of how many times the helper runs.
//
// All helpers are conservative on missing canonical form: they never delete
// the only copy of a Playtime/UserGameStats file. This matters for installs
// that are mid-migration (legacy exists, canonical not yet written).

namespace LegacyMetadataCleanup {

struct SweepStats {
    int filesRemoved = 0;
    int dirsRemoved = 0;
    int errors = 0;
};

// Layer 1: Sweep `Steam\userdata\{acct}\{app}\remote\` for every account and
// app directory. Unconditionally removes:
//   - top-level `Playtime.bin`, `UserGameStats.bin`
//   - entire `.cloudredirect\` subdirectory (wrong location; current DLL writes
//     canonical metadata into our private blob cache, not into Steam's remote
//     dir).
//
// `steamPath` must be the Steam root with a trailing backslash (matches the
// convention used by cloud_intercept/cloud_storage). Missing `userdata\` is a
// successful no-op (count = 0).
SweepStats PruneSteamUserdata(const std::string& steamPath);

// Layer 2: Sweep `{localRoot}storage\{acct}\{app}\` for every account and app
// directory. Removes top-level `Playtime.bin`/`UserGameStats.bin` ONLY when
// the canonical `.cloudredirect\{same}` sibling exists (never delete the
// only copy).
//
// `localRoot` must end with a backslash. Missing `storage\` is a successful
// no-op.
SweepStats PruneLocalBlobCache(const std::string& localRoot);

// Layer 3 helper (pure): given the raw blob paths from a provider listing
// `{acct}/{app}/blobs/{name}`, classify which legacy-named entries should be
// deleted. A legacy entry is returned only when a canonical sibling with the
// same `{acct}/{app}` prefix is also present in the listing. The returned
// paths are the RAW legacy paths, suitable for `ICloudProvider::Remove` or
// enqueueing a `WorkItem::Delete`.
//
// Caller must only invoke this on a verified-complete listing. Classification
// on a partial listing could miss the canonical sibling and return false
// positives that would destroy the only cloud copy.
std::vector<std::string> ClassifyLegacyCloudBlobsToDelete(
    const std::vector<std::string>& cloudBlobRawPaths);

} // namespace LegacyMetadataCleanup
