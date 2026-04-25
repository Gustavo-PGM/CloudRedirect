#pragma once
// Cloud-storage paths for CloudRedirect's internal metadata blobs.
// Split out of cloud_intercept.h so cloud_storage and other low-level
// modules can consume just the path constants without pulling in the
// full intercept-layer interface (CNetPacket, hook installers, etc.).

namespace CloudIntercept {

// Canonical (current) paths used for newly-written metadata blobs.
inline constexpr const char* kPlaytimeMetadataPath = ".cloudredirect/Playtime.bin";
inline constexpr const char* kStatsMetadataPath    = ".cloudredirect/UserGameStats.bin";

// Legacy paths from earlier CloudRedirect builds that did not namespace
// metadata under .cloudredirect/. Still recognized when classifying
// existing cloud blobs for canonicalization or cleanup.
inline constexpr const char* kLegacyPlaytimeMetadataPath = "Playtime.bin";
inline constexpr const char* kLegacyStatsMetadataPath    = "UserGameStats.bin";

} // namespace CloudIntercept
