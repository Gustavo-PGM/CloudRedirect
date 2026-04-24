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
    /// for having obtained user confirmation &#8212; this method performs no
    /// safety checks of its own.
    /// </summary>
    public async Task<PruneResult> PruneAsync(
        string accountId, string appId, IReadOnlyCollection<string> orphanFilenames,
        CancellationToken cancel = default)
    {
        if (orphanFilenames.Count == 0)
            return new PruneResult(0, 0, Array.Empty<string>(), null);

        _log?.Invoke($"[OrphanBlob] Pruning {orphanFilenames.Count} orphan blob(s) for account {accountId} app {appId}");

        CloudProviderClient.DeleteBlobsResult result;
        try
        {
            result = await _client.DeleteAppBlobsAsync(accountId, appId, orphanFilenames, cancel);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _log?.Invoke($"[OrphanBlob] Prune threw: {ex.Message}");
            return new PruneResult(0, orphanFilenames.Count, orphanFilenames.ToList(), ex.Message);
        }

        _log?.Invoke($"[OrphanBlob] Prune complete: deleted={result.Deleted} failed={result.Failed}");
        return new PruneResult(result.Deleted, result.Failed, result.FailedFilenames, result.Error);
    }

    /// <summary>
    /// Pure function: given an unordered cloud-side blob list and the set of
    /// filenames referenced by <c>file_tokens.dat</c>, return the sorted list
    /// of orphan filenames. Case-sensitive comparison matches the native
    /// side's <c>std::unordered_map&lt;std::string,...&gt;</c> keying.
    /// Duplicates in the input are deduplicated so the prune step only
    /// issues one delete per filename.
    ///
    /// CANONICALIZATION NOTE: The native side maps legacy metadata filenames
    /// (e.g. <c>Playtime.bin</c>) to canonical <c>.cloudredirect/*</c> paths
    /// via <c>CanonicalizeInternalMetadataName</c> in
    /// <c>src/cloud_storage.cpp:20-28</c>. Canonical names contain a path
    /// separator, so they live inside a subfolder of <c>blobs/</c> and are
    /// never returned by this UI's top-level listing &#8212; which is exactly
    /// why they cannot be falsely flagged as orphans here. Legacy names at
    /// the blobs root ARE listed, and if migration has moved the key to the
    /// canonical form in <c>file_tokens.dat</c>, the legacy blob is
    /// correctly treated as a stale orphan. IMPORTANT: any future
    /// canonicalization that keeps the blob at the top level of
    /// <c>blobs/</c> must be mirrored here, otherwise the UI will
    /// incorrectly flag live files as orphans.
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
            if (!referenced.Contains(name)) orphans.Add(name);
        }
        orphans.Sort(StringComparer.Ordinal);
        return orphans;
    }
}
