using System.Net.Http;

namespace CloudRedirect.Services;

/// <summary>
/// Lightweight cloud provider client for the UI.
/// Supports deleting app data and pruning orphan blobs from Google Drive,
/// OneDrive, or a folder provider. Reads the same DPAPI-encrypted token
/// files as the DLL.
///
/// <para>This class is a thin facade: it reads the active <see cref="CloudConfig"/>,
/// resolves the matching <see cref="IUiCloudProvider"/> via
/// <see cref="UiCloudProviderFactory"/>, and forwards calls. The actual
/// per-provider HTTP / filesystem logic lives in the
/// <c>CloudRedirect.Services.Providers</c> namespace so each backend can be
/// reasoned about in isolation.</para>
///
/// <para>The result types remain nested here because <c>OrphanBlobService</c>
/// references them by the <c>CloudProviderClient.{Foo}Result</c> path; moving
/// them would force unrelated caller churn for no benefit.</para>
/// </summary>
internal sealed class CloudProviderClient : IDisposable
{
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(30) };
    private readonly Action<string>? _log;

    public CloudProviderClient(Action<string>? log = null)
    {
        _log = log;
    }

    /// <summary>
    /// Result of a cloud deletion operation.
    /// </summary>
    public record DeleteResult(bool Success, int FilesDeleted, string? Error);

    /// <summary>
    /// Deletes all cloud data for a specific app. Reads the config to determine
    /// the provider type and delegates to the appropriate implementation.
    /// Returns a result indicating success/failure and files deleted.
    /// </summary>
    public async Task<DeleteResult> DeleteAppDataAsync(string accountId, string appId, CancellationToken cancel = default)
    {
        var config = SteamDetector.ReadConfig();
        var provider = UiCloudProviderFactory.TryResolve(config, _http, _log);
        if (provider == null)
            return new DeleteResult(true, 0, null); // no config / local-only / unrecognized -> nothing to do

        try
        {
            return await provider.DeleteAppDataAsync(accountId, appId, cancel);
        }
        catch (Exception ex)
        {
            return new DeleteResult(false, 0, ex.Message);
        }
    }

    /// <summary>
    /// Returns the display name of the configured cloud provider, or null if local-only.
    /// </summary>
    public static string? GetProviderDisplayName()
    {
        var config = SteamDetector.ReadConfig();
        if (config == null || config.IsLocal) return null;
        return config.DisplayName;
    }

    // Orphan-blob listing / deletion
    //
    // Cloud blobs live under {provider}/CloudRedirect/{accountId}/{appId}/blobs/
    // (gdrive/onedrive) or {syncPath}/{accountId}/{appId}/blobs/ (folder). They
    // are the per-file save payloads Steam uploaded via the intercept path.
    //
    // ListAppBlobsAsync / DeleteAppBlobsAsync power the UI's prune-orphan
    // action. ListAppBlobsAsync returns a Complete flag; it is false when
    // pagination, HTTP, or provider errors interrupt enumeration. Callers
    // must not delete based on an incomplete listing -- a partial result
    // can make legitimate blobs look orphaned. Mirrors the native
    // ICloudProvider::ListChecked discipline in src/cloud_storage.cpp.

    /// <summary>
    /// Result of listing blob filenames under a single app on the cloud.
    /// </summary>
    /// <param name="BlobFilenames">Direct children of the app's blobs folder
    /// that are files (subfolders excluded). Case-sensitive; order unspecified.</param>
    /// <param name="Complete">True iff enumeration ran to conclusion with no
    /// skipped pages and no provider errors. See the completeness contract
    /// in the CloudProviderClient header comment.</param>
    /// <param name="Error">Non-null when a listing issue occurred; may be set
    /// even if Complete is true (e.g. auth refresh warning).</param>
    public record ListBlobsResult(IReadOnlyList<string> BlobFilenames, bool Complete, string? Error);

    /// <summary>
    /// Result of deleting a specific set of blob filenames.
    /// </summary>
    public record DeleteBlobsResult(int Deleted, int Failed, IReadOnlyList<string> FailedFilenames, string? Error);

    /// <summary>
    /// Enumerate direct blob filenames under <c>CloudRedirect/{accountId}/{appId}/blobs/</c>.
    /// Always returns <c>Complete = true</c> for the local folder provider and
    /// when no cloud is configured. For gdrive/onedrive, Complete is false on
    /// any pagination, auth, or transport failure; see the completeness
    /// contract in the type comment.
    /// </summary>
    public async Task<ListBlobsResult> ListAppBlobsAsync(string accountId, string appId, CancellationToken cancel = default)
    {
        var config = SteamDetector.ReadConfig();
        var provider = UiCloudProviderFactory.TryResolve(config, _http, _log);
        if (provider == null)
            return new ListBlobsResult(Array.Empty<string>(), true, null);

        try
        {
            return await provider.ListAppBlobsAsync(accountId, appId, cancel);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new ListBlobsResult(Array.Empty<string>(), false, ex.Message);
        }
    }

    /// <summary>
    /// Delete a specific set of blob filenames under <c>CloudRedirect/{accountId}/{appId}/blobs/</c>.
    /// Filenames that do not exist on the cloud are treated as already-deleted
    /// (counted toward <c>Deleted</c>) to keep prune operations idempotent
    /// across retries. Filenames containing path separators or <c>..</c> are
    /// rejected before any I/O as a defense-in-depth guard.
    /// </summary>
    public async Task<DeleteBlobsResult> DeleteAppBlobsAsync(
        string accountId, string appId, IReadOnlyCollection<string> blobFilenames, CancellationToken cancel = default)
    {
        if (blobFilenames.Count == 0)
            return new DeleteBlobsResult(0, 0, Array.Empty<string>(), null);

        // Defense-in-depth: reject path-like filenames up front. The cloud
        // side should never produce them (our own writes use bare filenames),
        // but a defensive check here means a malicious or corrupt listing
        // cannot coerce the folder provider into deleting files outside the
        // target blobs/ directory. Per-provider implementations rely on this
        // pre-filtering and do NOT re-validate.
        var safe = new List<string>(blobFilenames.Count);
        var rejected = new List<string>();
        foreach (var name in blobFilenames)
        {
            if (IsUnsafeBlobName(name)) rejected.Add(name);
            else safe.Add(name);
        }

        var config = SteamDetector.ReadConfig();
        var provider = UiCloudProviderFactory.TryResolve(config, _http, _log);
        if (provider == null)
        {
            // No cloud configured -- nothing to do on the remote side.
            // Rejected names are still reported as failed so the caller can
            // surface them in the UI diagnostics.
            string? err = rejected.Count > 0 ? $"{rejected.Count} filename(s) rejected as unsafe." : null;
            return new DeleteBlobsResult(0, rejected.Count, rejected, err);
        }

        DeleteBlobsResult inner;
        try
        {
            inner = await provider.DeleteAppBlobsAsync(accountId, appId, safe, cancel);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            var all = new List<string>(safe);
            all.AddRange(rejected);
            return new DeleteBlobsResult(0, all.Count, all, ex.Message);
        }

        if (rejected.Count == 0) return inner;

        // Fold rejected names into the failure tally.
        var mergedFailed = new List<string>(inner.FailedFilenames);
        mergedFailed.AddRange(rejected);
        var mergedErr = inner.Error;
        if (mergedErr == null) mergedErr = $"{rejected.Count} filename(s) rejected as unsafe.";
        else mergedErr += $" {rejected.Count} filename(s) also rejected as unsafe.";
        return new DeleteBlobsResult(inner.Deleted, inner.Failed + rejected.Count, mergedFailed, mergedErr);
    }

    /// <summary>
    /// Rejects blob filenames Windows path canonicalization would rewrite.
    /// Trailing dots/spaces are the main hazard: Linux-backed providers store
    /// them but Path.GetFullPath strips them, so deleting "foo." would target
    /// "foo" instead. Also rejects path separators, parent traversal, and the
    /// reserved DOS device names.
    /// </summary>
    private static bool IsUnsafeBlobName(string name)
    {
        if (string.IsNullOrEmpty(name)) return true;
        if (name.Contains('/') || name.Contains('\\')) return true;
        if (name == "." || name == "..") return true;
        if (name.Contains(":")) return true; // drive-letter / NTFS stream
        // Windows canonicalization hazard: trailing '.' or ' ' is silently
        // stripped, causing "foo." / "foo " to map to the local file "foo".
        if (name.EndsWith('.') || name.EndsWith(' ')) return true;
        // Control characters (including embedded NUL) are invalid in
        // Windows filenames; cloud providers may accept them, but
        // File.Delete would throw and they have no legitimate use in a
        // Steam save filename.
        foreach (var c in name)
            if (c < 0x20) return true;
        // Windows reserved device names (with or without extension) are
        // interpreted as the device rather than a file on the folder
        // provider path: File.Exists returns false silently and our loop
        // would count them as idempotent-deleted successes. Reject at
        // the boundary so the UI reports them as failed instead.
        var baseName = name;
        var dot = name.IndexOf('.');
        if (dot >= 0) baseName = name.Substring(0, dot);
        if (s_reservedDeviceNames.Contains(baseName)) return true;
        return false;
    }

    private static readonly HashSet<string> s_reservedDeviceNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "CON", "PRN", "AUX", "NUL",
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    };

    public void Dispose()
    {
        _http.Dispose();
    }
}
