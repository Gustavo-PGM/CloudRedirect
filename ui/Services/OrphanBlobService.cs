using System.IO;

namespace CloudRedirect.Services;

/// <summary>
/// Orchestrates detection and pruning of orphan cloud blobs for a single app.
///
/// A blob is a file under the cloud path
/// <c>{accountId}/{appId}/blobs/{filename}</c>. An "orphan" is a cloud blob
/// whose filename is NOT present as a key in the local <c>file_tokens.dat</c>.
/// That file is the authoritative record of which cloud blobs the DLL
/// believes are still referenced by its root-token &#215; filename mapping;
/// any blob on the cloud that is not in it was uploaded by a prior session,
/// a failed delete, or a manual override, and will never be downloaded or
/// cleaned up by the normal sync path.
///
/// CRITICAL SAFETY CONTRACT: <see cref="ScanAsync"/> results carry a
/// <see cref="ScanResult.ListingComplete"/> flag. Callers MUST refuse to
/// prune when it is false &#8212; a partial cloud listing could make
/// legitimate blobs appear orphaned, causing permanent save loss. This
/// mirrors the native-side ICloudProvider::ListChecked completeness
/// discipline at <c>src/cloud_storage.cpp:1283-1566</c>.
/// </summary>
internal sealed class OrphanBlobService
{
    private readonly CloudProviderClient _client;
    private readonly string _steamPath;
    private readonly Action<string>? _log;

    public OrphanBlobService(CloudProviderClient client, string steamPath, Action<string>? log = null)
    {
        _client = client;
        _steamPath = steamPath;
        _log = log;
    }

    /// <summary>
    /// Result of a scan. <see cref="Orphans"/> is non-empty only when
    /// <see cref="ListingComplete"/> is true &#8212; a partial listing is
    /// treated as "cannot determine orphan set".
    /// </summary>
    public record ScanResult(
        IReadOnlyList<string> Orphans,
        int TotalCloudBlobs,
        int ReferencedCount,
        bool ListingComplete,
        string? Error);

    /// <summary>
    /// Result of a prune. Idempotent with respect to already-deleted blobs:
    /// a filename that is absent on the cloud is counted toward
    /// <see cref="Deleted"/>, not <see cref="Failed"/>.
    /// </summary>
    public record PruneResult(
        int Deleted,
        int Failed,
        IReadOnlyList<string> FailedFilenames,
        string? Error);

    /// <summary>
    /// Scan the cloud for blobs under <paramref name="accountId"/>/
    /// <paramref name="appId"/> and return which filenames are unreferenced
    /// by the local <c>file_tokens.dat</c>. Does NOT delete anything. Only
    /// call <see cref="PruneAsync"/> after verifying
    /// <see cref="ScanResult.ListingComplete"/> is true.
    /// </summary>
    public async Task<ScanResult> ScanAsync(string accountId, string appId, CancellationToken cancel = default)
    {
        _log?.Invoke($"[OrphanBlob] Scanning cloud blobs for account {accountId} app {appId}...");

        CloudProviderClient.ListBlobsResult listing;
        try
        {
            listing = await _client.ListAppBlobsAsync(accountId, appId, cancel);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _log?.Invoke($"[OrphanBlob] Cloud listing threw: {ex.Message}");
            return new ScanResult(Array.Empty<string>(), 0, 0, false, ex.Message);
        }

        // If listing is incomplete or errored, refuse to compute orphans so
        // the caller cannot accidentally prune based on partial data.
        if (!listing.Complete || listing.Error != null)
        {
            _log?.Invoke($"[OrphanBlob] Cloud listing incomplete ({listing.BlobFilenames.Count} partial, error={listing.Error ?? "none"})");
            return new ScanResult(
                Array.Empty<string>(),
                listing.BlobFilenames.Count,
                0,
                listing.Complete,
                listing.Error);
        }

        HashSet<string> referenced;
        try
        {
            var ftPath = Path.Combine(_steamPath, "cloud_redirect", "storage", accountId, appId, "file_tokens.dat");
            referenced = FileTokensParser.ReadFromDisk(ftPath);
        }
        catch (Exception ex)
        {
            _log?.Invoke($"[OrphanBlob] Failed to read file_tokens.dat: {ex.Message}");
            // Safer to abort than assume zero referenced filenames &#8212; a
            // false-empty referenced set would mark every cloud blob as an
            // orphan.
            return new ScanResult(Array.Empty<string>(), listing.BlobFilenames.Count, 0, false,
                $"Could not read file_tokens.dat: {ex.Message}");
        }

        var orphans = ComputeOrphans(listing.BlobFilenames, referenced);
        _log?.Invoke($"[OrphanBlob] Scan complete: cloud={listing.BlobFilenames.Count} referenced={referenced.Count} orphans={orphans.Count}");
        return new ScanResult(orphans, listing.BlobFilenames.Count, referenced.Count, true, null);
    }

    /// <summary>
    /// Delete the given set of orphan filenames from the cloud. Callers are
    /// responsible for having just obtained these filenames from a
    /// <see cref="ScanAsync"/> result with <c>ListingComplete = true</c> and
    /// for having obtained user confirmation.
    ///
    /// DEFENSE-IN-DEPTH: This method independently enforces the
    /// <see cref="InternalMetadataFilenames"/> whitelist before calling the
    /// provider. <see cref="ComputeOrphans"/> already filters at scan time, but
    /// a future caller, a buggy binding, a stale persisted orphan list, or a
    /// scan-vs-prune list mutation could still feed a metadata filename here;
    /// without this second check the provider would happily delete live DLL
    /// playtime/stats state. The destructive boundary is the right place for
    /// a save-loss-critical invariant to live.
    /// </summary>
    public async Task<PruneResult> PruneAsync(
        string accountId, string appId, IReadOnlyCollection<string> orphanFilenames,
        CancellationToken cancel = default)
    {
        if (orphanFilenames.Count == 0)
            return new PruneResult(0, 0, Array.Empty<string>(), null);

        // Whitelist-filter at the destructive boundary. Empty / null filenames
        // are also dropped here so the provider never sees a request for
        // ambiguous state.
        var filtered = new List<string>(orphanFilenames.Count);
        int skipped = 0;
        foreach (var name in orphanFilenames)
        {
            if (string.IsNullOrEmpty(name)) { skipped++; continue; }
            if (InternalMetadataFilenames.Contains(name)) { skipped++; continue; }
            filtered.Add(name);
        }
        if (skipped > 0)
            _log?.Invoke($"[OrphanBlob] Refused to prune {skipped} invalid/metadata filename(s) at destructive boundary");
        if (filtered.Count == 0)
            return new PruneResult(0, 0, Array.Empty<string>(), null);

        _log?.Invoke($"[OrphanBlob] Pruning {filtered.Count} orphan blob(s) for account {accountId} app {appId}");

        CloudProviderClient.DeleteBlobsResult result;
        try
        {
            result = await _client.DeleteAppBlobsAsync(accountId, appId, filtered, cancel);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _log?.Invoke($"[OrphanBlob] Prune threw: {ex.Message}");
            return new PruneResult(0, filtered.Count, filtered, ex.Message);
        }

        _log?.Invoke($"[OrphanBlob] Prune complete: deleted={result.Deleted} failed={result.Failed}");
        return new PruneResult(result.Deleted, result.Failed, result.FailedFilenames, result.Error);
    }

    /// <summary>
    /// DLL-internal metadata filenames that must NEVER be reported as orphans
    /// regardless of <c>file_tokens.dat</c> state. These are the four forms
    /// the native side treats as metadata in the exclusion filter at
    /// <c>src/rpc_handlers.cpp:57-58</c>: canonical <c>.cloudredirect/*.bin</c>
    /// plus their pre-canonicalization legacy top-level names
    /// (<c>src/cloud_intercept.h:6-9</c>).
    ///
    /// Why this exists: the native <c>CanonicalizeInternalMetadataName</c>
    /// rewrites legacy top-level metadata reads/writes to the canonical
    /// subfolder path, so <c>file_tokens.dat</c> only keys the canonical
    /// form. But older sessions (or cross-machine sync from an unmigrated
    /// peer) can leave a legacy top-level blob on the cloud, and the
    /// top-level <c>blobs/</c> listing in this UI will return it. A pure
    /// set-difference against <c>referenced</c> would then mark it as an
    /// orphan &#8212; triggering a UI prune would delete live playtime /
    /// stats metadata the DLL still relies on. These filenames stay under
    /// the DLL's authority; UI orphan logic is scoped to user save files.
    /// Kept in lockstep with <c>src/rpc_handlers.cpp:IsInternalMetadata</c>;
    /// adding a new metadata path on the native side requires mirroring it
    /// here.
    /// </summary>
    internal static readonly IReadOnlySet<string> InternalMetadataFilenames =
        new HashSet<string>(StringComparer.Ordinal)
        {
            ".cloudredirect/Playtime.bin",
            ".cloudredirect/UserGameStats.bin",
            "Playtime.bin",
            "UserGameStats.bin",
        };

    /// <summary>
    /// Pure function: given an unordered cloud-side blob list and the set of
    /// filenames referenced by <c>file_tokens.dat</c>, return the sorted list
    /// of orphan filenames. Case-sensitive comparison matches the native
    /// side's <c>std::unordered_map&lt;std::string,...&gt;</c> keying.
    /// Duplicates in the input are deduplicated so the prune step only
    /// issues one delete per filename.
    ///
    /// INTERNAL METADATA: Filenames in <see cref="InternalMetadataFilenames"/>
    /// are unconditionally skipped &#8212; they are DLL-managed, not user
    /// save data, and pruning them would corrupt playtime / achievement
    /// state. See that set's doc comment for the full rationale.
    /// </summary>
    internal static List<string> ComputeOrphans(
        IEnumerable<string> cloudBlobFilenames,
        IReadOnlySet<string> referenced)
    {
        var orphans = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in cloudBlobFilenames)
        {
            if (string.IsNullOrEmpty(name)) continue;
            if (!seen.Add(name)) continue; // dedup
            if (InternalMetadataFilenames.Contains(name)) continue; // DLL-internal, never prune
            if (!referenced.Contains(name)) orphans.Add(name);
        }
        orphans.Sort(StringComparer.Ordinal);
        return orphans;
    }
}
