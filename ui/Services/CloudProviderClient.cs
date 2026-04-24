using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace CloudRedirect.Services;

/// <summary>
/// Lightweight cloud provider client for the UI.
/// Supports deleting app data from Google Drive, OneDrive, or a folder provider.
/// Reads the same DPAPI-encrypted token files as the DLL.
/// </summary>
internal sealed class CloudProviderClient : IDisposable
{
    // Same credentials as OAuthService / DLL (public clients)
    private const string GDriveClientId     = "1072944905499-vm2v2i5dvn0a0d2o4ca36i1vge8cvbn0.apps.googleusercontent.com";
    private const string GDriveClientSecret = "v6V3fKV_zWU7iw1DrpO1rknX";
    private const string GDriveTokenUrl     = "https://oauth2.googleapis.com/token";

    private const string OneDriveClientId   = "c582f799-5dc5-48a7-a4cd-cd0d8af354a2";
    private const string OneDriveTokenUrl   = "https://login.microsoftonline.com/consumers/oauth2/v2.0/token";

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
        if (config == null)
            return new DeleteResult(true, 0, null); // no config = local-only, nothing to do

        if (config.IsLocal)
            return new DeleteResult(true, 0, null); // local-only mode, no cloud

        try
        {
            return config.Provider switch
            {
                "gdrive"  => await DeleteGDriveAppAsync(config.TokenPath!, accountId, appId, cancel),
                "onedrive" => await DeleteOneDriveAppAsync(config.TokenPath!, accountId, appId, cancel),
                "folder"  => DeleteFolderApp(config.SyncPath!, accountId, appId),
                _         => new DeleteResult(true, 0, null)
            };
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

    // ---- Google Drive ----

    private async Task<DeleteResult> DeleteGDriveAppAsync(
        string tokenPath, string accountId, string appId, CancellationToken cancel)
    {
        var token = await GetGDriveAccessTokenAsync(tokenPath, cancel);
        if (token == null)
            return new DeleteResult(false, 0, "Failed to get Google Drive access token. Re-authenticate in Cloud Provider settings.");

        // Walk the folder hierarchy: CloudRedirect -> {accountId} -> {appId}
        var rootId = await FindGDriveFolder(token, "CloudRedirect", "root", cancel);
        if (rootId == null)
        {
            _log?.Invoke("No CloudRedirect folder found on Google Drive -- nothing to delete.");
            return new DeleteResult(true, 0, null);
        }

        var accountFolderId = await FindGDriveFolder(token, accountId, rootId, cancel);
        if (accountFolderId == null)
        {
            _log?.Invoke($"No account folder '{accountId}' found on Google Drive -- nothing to delete.");
            return new DeleteResult(true, 0, null);
        }

        var appFolderId = await FindGDriveFolder(token, appId, accountFolderId, cancel);
        if (appFolderId == null)
        {
            _log?.Invoke($"No app folder '{appId}' found on Google Drive -- nothing to delete.");
            return new DeleteResult(true, 0, null);
        }

        // Recursively delete all files, then folders bottom-up.
        // We can't just DELETE the folder because drive.file scope requires
        // write access to ALL children -- which fails if any child is from
        // a previous OAuth session (appNotAuthorizedToChild).
        _log?.Invoke($"Recursively deleting Google Drive folder for app {appId}...");
        var (deleted, failed) = await DeleteGDriveFolderRecursive(token, appFolderId, cancel);
        _log?.Invoke($"Deleted {deleted} item(s) from Google Drive ({failed} failed).");

        if (failed > 0 && deleted == 0)
            return new DeleteResult(false, 0, $"Could not delete any files from Google Drive ({failed} failed). Check Cloud Provider auth.");

        return new DeleteResult(true, deleted, failed > 0 ? $"{failed} file(s) could not be deleted (may require re-authentication)." : null);
    }

    /// <summary>
    /// Recursively deletes all children of a folder, then the folder itself.
    /// Returns (deletedCount, failedCount).
    /// </summary>
    private async Task<(int Deleted, int Failed)> DeleteGDriveFolderRecursive(
        string token, string folderId, CancellationToken cancel)
    {
        int deleted = 0;
        int failed = 0;

        var children = await ListGDriveFolderChildren(token, folderId, cancel);

        foreach (var child in children)
        {
            if (child.IsFolder)
            {
                // Recurse into subfolders first
                var (subDel, subFail) = await DeleteGDriveFolderRecursive(token, child.Id, cancel);
                deleted += subDel;
                failed += subFail;
            }
            else
            {
                var (ok, _, _) = await DeleteGDriveItem(token, child.Id, cancel);
                if (ok) deleted++;
                else failed++;
            }
        }

        // Now try to delete the (hopefully empty) folder itself
        var (folderOk, _, _) = await DeleteGDriveItem(token, folderId, cancel);
        if (folderOk) deleted++;
        else failed++;

        return (deleted, failed);
    }

    /// <summary>
    /// Lists all direct children of a Google Drive folder (files and subfolders).
    /// </summary>
    private async Task<List<GDriveChild>> ListGDriveFolderChildren(
        string token, string folderId, CancellationToken cancel)
    {
        var result = new List<GDriveChild>();
        string? pageToken = null;

        do
        {
            var query = $"'{folderId}' in parents and trashed=false";
            var url = $"https://www.googleapis.com/drive/v3/files?q={Uri.EscapeDataString(query)}" +
                      "&fields=nextPageToken,files(id,mimeType)&pageSize=1000";
            if (pageToken != null)
                url += $"&pageToken={Uri.EscapeDataString(pageToken)}";

            var req = new HttpRequestMessage(HttpMethod.Get, url);
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

            var resp = await _http.SendAsync(req, cancel);
            if (!resp.IsSuccessStatusCode) break;

            var json = await resp.Content.ReadAsStringAsync(cancel);
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("files", out var files))
            {
                foreach (var file in files.EnumerateArray())
                {
                    var id = file.GetProperty("id").GetString()!;
                    var mime = file.TryGetProperty("mimeType", out var mt) ? mt.GetString() : "";
                    result.Add(new GDriveChild(id, mime == "application/vnd.google-apps.folder"));
                }
            }

            pageToken = doc.RootElement.TryGetProperty("nextPageToken", out var npt)
                ? npt.GetString() : null;
        } while (pageToken != null);

        return result;
    }

    private record GDriveChild(string Id, bool IsFolder);

    private async Task<string?> GetGDriveAccessTokenAsync(string tokenPath, CancellationToken cancel)
    {
        var json = TokenFile.ReadJson(tokenPath);
        if (json == null) return null;

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var accessToken = root.TryGetProperty("access_token", out var at) ? at.GetString() : null;
        var refreshToken = root.TryGetProperty("refresh_token", out var rt) ? rt.GetString() : null;
        var expiresAt = root.TryGetProperty("expires_at", out var ea) ? ea.GetInt64() : 0;

        if (string.IsNullOrEmpty(refreshToken)) return null;

        // If token is still valid (with 60s buffer), use it
        if (!string.IsNullOrEmpty(accessToken) && expiresAt > DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 60)
            return accessToken;

        // Refresh the token
        _log?.Invoke("Refreshing Google Drive access token...");
        var body = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["client_id"] = GDriveClientId,
            ["client_secret"] = GDriveClientSecret,
            ["refresh_token"] = refreshToken,
            ["grant_type"] = "refresh_token"
        });

        var resp = await _http.PostAsync(GDriveTokenUrl, body, cancel);
        if (!resp.IsSuccessStatusCode) return null;

        var respJson = await resp.Content.ReadAsStringAsync(cancel);
        using var respDoc = JsonDocument.Parse(respJson);
        var respRoot = respDoc.RootElement;

        var newAccessToken = respRoot.TryGetProperty("access_token", out var nat) ? nat.GetString() : null;
        var expiresIn = respRoot.TryGetProperty("expires_in", out var ei) ? ei.GetInt64() : 3600;

        if (string.IsNullOrEmpty(newAccessToken)) return null;

        // Save updated token back to file
        try
        {
            var newToken = new
            {
                access_token = newAccessToken,
                refresh_token = refreshToken,
                expires_at = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + expiresIn
            };
            TokenFile.WriteJson(tokenPath, JsonSerializer.Serialize(newToken, new JsonSerializerOptions { WriteIndented = true }));
        }
        catch { /* best-effort save */ }

        return newAccessToken;
    }

    private async Task<string?> FindGDriveFolder(string token, string name, string parentId, CancellationToken cancel)
    {
        var escapedName = name.Replace("'", "\\'");
        var query = $"name='{escapedName}' and '{parentId}' in parents " +
                    "and mimeType='application/vnd.google-apps.folder' and trashed=false";
        var url = $"https://www.googleapis.com/drive/v3/files?q={Uri.EscapeDataString(query)}" +
                  "&fields=files(id,name)&orderBy=createdTime&pageSize=10";

        var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        var resp = await _http.SendAsync(req, cancel);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(cancel);
            _log?.Invoke($"GDrive FindFolder '{name}' in {parentId}: HTTP {(int)resp.StatusCode}: {body}");
            return null;
        }

        var json = await resp.Content.ReadAsStringAsync(cancel);
        using var doc = JsonDocument.Parse(json);

        if (!doc.RootElement.TryGetProperty("files", out var files)) return null;
        if (files.GetArrayLength() == 0) return null;

        return files[0].GetProperty("id").GetString();
    }

    private async Task<(bool Ok, int StatusCode, string? Body)> DeleteGDriveItem(
        string token, string fileId, CancellationToken cancel)
    {
        var req = new HttpRequestMessage(HttpMethod.Delete,
            $"https://www.googleapis.com/drive/v3/files/{fileId}");
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        var resp = await _http.SendAsync(req, cancel);
        int status = (int)resp.StatusCode;
        if (resp.IsSuccessStatusCode || resp.StatusCode == HttpStatusCode.NotFound)
            return (true, status, null);

        var body = await resp.Content.ReadAsStringAsync(cancel);
        _log?.Invoke($"GDrive DELETE {fileId} failed: HTTP {status}: {body}");
        return (false, status, body);
    }

    // ---- OneDrive ----

    private async Task<DeleteResult> DeleteOneDriveAppAsync(
        string tokenPath, string accountId, string appId, CancellationToken cancel)
    {
        var token = await GetOneDriveAccessTokenAsync(tokenPath, cancel);
        if (token == null)
            return new DeleteResult(false, 0, "Failed to get OneDrive access token. Re-authenticate in Cloud Provider settings.");

        // OneDrive is path-based -- delete the app folder directly
        var folderPath = $"CloudRedirect/{accountId}/{appId}";
        var encodedPath = string.Join("/", folderPath.Split('/').Select(Uri.EscapeDataString));

        // First check if the folder exists and count children
        int fileCount = await CountOneDriveFolderFiles(token, encodedPath, cancel);

        // Delete the folder
        _log?.Invoke($"Deleting OneDrive folder for app {appId} ({fileCount} files)...");
        var req = new HttpRequestMessage(HttpMethod.Delete,
            $"https://graph.microsoft.com/v1.0/me/drive/root:/{encodedPath}:");
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        var resp = await _http.SendAsync(req, cancel);
        if (!resp.IsSuccessStatusCode && resp.StatusCode != HttpStatusCode.NotFound)
            return new DeleteResult(false, 0, $"OneDrive delete failed (HTTP {(int)resp.StatusCode}).");

        _log?.Invoke($"Deleted {fileCount} files from OneDrive.");
        return new DeleteResult(true, fileCount, null);
    }

    private async Task<string?> GetOneDriveAccessTokenAsync(string tokenPath, CancellationToken cancel)
    {
        var json = TokenFile.ReadJson(tokenPath);
        if (json == null) return null;

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var accessToken = root.TryGetProperty("access_token", out var at) ? at.GetString() : null;
        var refreshToken = root.TryGetProperty("refresh_token", out var rt) ? rt.GetString() : null;
        var expiresAt = root.TryGetProperty("expires_at", out var ea) ? ea.GetInt64() : 0;

        if (string.IsNullOrEmpty(refreshToken)) return null;

        if (!string.IsNullOrEmpty(accessToken) && expiresAt > DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 60)
            return accessToken;

        // Refresh
        _log?.Invoke("Refreshing OneDrive access token...");
        var body = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["client_id"] = OneDriveClientId,
            ["refresh_token"] = refreshToken,
            ["grant_type"] = "refresh_token",
            ["scope"] = "Files.ReadWrite offline_access"
        });

        var resp = await _http.PostAsync(OneDriveTokenUrl, body, cancel);
        if (!resp.IsSuccessStatusCode) return null;

        var respJson = await resp.Content.ReadAsStringAsync(cancel);
        using var respDoc = JsonDocument.Parse(respJson);
        var respRoot = respDoc.RootElement;

        var newAccessToken = respRoot.TryGetProperty("access_token", out var nat) ? nat.GetString() : null;
        var newRefreshToken = respRoot.TryGetProperty("refresh_token", out var nrt) ? nrt.GetString() : refreshToken;
        var expiresIn = respRoot.TryGetProperty("expires_in", out var ei) ? ei.GetInt64() : 3600;

        if (string.IsNullOrEmpty(newAccessToken)) return null;

        try
        {
            var newToken = new
            {
                access_token = newAccessToken,
                refresh_token = newRefreshToken,
                expires_at = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + expiresIn
            };
            TokenFile.WriteJson(tokenPath, JsonSerializer.Serialize(newToken, new JsonSerializerOptions { WriteIndented = true }));
        }
        catch { /* best-effort save */ }

        return newAccessToken;
    }

    private async Task<int> CountOneDriveFolderFiles(string token, string encodedPath, CancellationToken cancel)
    {
        // Get the folder's children count
        var req = new HttpRequestMessage(HttpMethod.Get,
            $"https://graph.microsoft.com/v1.0/me/drive/root:/{encodedPath}:?$select=id,folder");
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        var resp = await _http.SendAsync(req, cancel);
        if (!resp.IsSuccessStatusCode) return 0;

        var json = await resp.Content.ReadAsStringAsync(cancel);
        using var doc = JsonDocument.Parse(json);

        if (doc.RootElement.TryGetProperty("folder", out var folder) &&
            folder.TryGetProperty("childCount", out var cc))
            return cc.GetInt32();

        return 0;
    }

    // ---- Folder provider ----

    private DeleteResult DeleteFolderApp(string syncPath, string accountId, string appId)
    {
        var folderPath = Path.Combine(syncPath, accountId, appId);
        if (!Directory.Exists(folderPath))
        {
            _log?.Invoke($"No folder provider data found at '{folderPath}'.");
            return new DeleteResult(true, 0, null);
        }

        var files = Directory.GetFiles(folderPath, "*", SearchOption.AllDirectories);
        int count = files.Length;

        _log?.Invoke($"Deleting {count} files from folder provider...");
        Directory.Delete(folderPath, true);
        _log?.Invoke($"Deleted {count} files from folder provider.");

        return new DeleteResult(true, count, null);
    }

    // ==== Orphan-blob listing / deletion =====================================
    //
    // Cloud blobs live under {provider}/CloudRedirect/{accountId}/{appId}/blobs/
    // (gdrive/onedrive) or {syncPath}/{accountId}/{appId}/blobs/ (folder). They
    // are the per-file save payloads Steam uploaded via the intercept path.
    //
    // ListAppBlobsAsync and DeleteAppBlobsAsync power the UI's "prune orphan
    // cloud blobs" action: scan cloud to find blob filenames for an app, and
    // delete a specific subset of them by filename.
    //
    // CRITICAL COMPLETENESS CONTRACT: ListAppBlobsAsync returns a `Complete`
    // flag. It is false if pagination did not run to conclusion, if an HTTP
    // error interrupted enumeration, or if the network call threw. Callers
    // MUST NOT make destructive decisions (e.g. "this blob is unreferenced,
    // delete it") when Complete is false &#8212; a partial listing could make
    // legitimate blobs appear orphaned. This mirrors the native-side
    // ICloudProvider::ListChecked completeness discipline enforced at
    // src/cloud_storage.cpp:1283-1566.

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
        if (config == null || config.IsLocal)
            return new ListBlobsResult(Array.Empty<string>(), true, null);

        try
        {
            return config.Provider switch
            {
                "gdrive"   => await ListGDriveAppBlobsAsync(config.TokenPath!, accountId, appId, cancel),
                "onedrive" => await ListOneDriveAppBlobsAsync(config.TokenPath!, accountId, appId, cancel),
                "folder"   => ListFolderAppBlobs(config.SyncPath!, accountId, appId),
                _          => new ListBlobsResult(Array.Empty<string>(), true, null)
            };
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
        // target blobs/ directory.
        var safe = new List<string>(blobFilenames.Count);
        var rejected = new List<string>();
        foreach (var name in blobFilenames)
        {
            if (IsUnsafeBlobName(name)) rejected.Add(name);
            else safe.Add(name);
        }

        var config = SteamDetector.ReadConfig();
        if (config == null || config.IsLocal)
        {
            // No cloud configured &#8212; nothing to do on the remote side.
            // Rejected names are still reported as failed so the caller can
            // surface them in the UI diagnostics.
            string? err = rejected.Count > 0 ? $"{rejected.Count} filename(s) rejected as unsafe." : null;
            return new DeleteBlobsResult(0, rejected.Count, rejected, err);
        }

        DeleteBlobsResult inner;
        try
        {
            inner = config.Provider switch
            {
                "gdrive"   => await DeleteGDriveAppBlobsAsync(config.TokenPath!, accountId, appId, safe, cancel),
                "onedrive" => await DeleteOneDriveAppBlobsAsync(config.TokenPath!, accountId, appId, safe, cancel),
                "folder"   => DeleteFolderAppBlobs(config.SyncPath!, accountId, appId, safe),
                _          => new DeleteBlobsResult(0, 0, Array.Empty<string>(), null)
            };
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
    /// A blob filename is unsafe if it contains path separators, a parent
    /// traversal fragment, or a component that Windows' path canonicalization
    /// would silently rewrite. <see cref="Path.Combine"/> + <see cref="Path.GetFullPath"/>
    /// on such input can escape the intended directory or, worse, silently
    /// resolve to a DIFFERENT file under the blobs folder &#8212; for example,
    /// Windows strips trailing dots and spaces when canonicalizing a path
    /// component, so <c>Path.GetFullPath(blobsDir + "\\foo.")</c> resolves to
    /// <c>blobsDir\foo</c> and <see cref="File.Delete"/> would erase the
    /// legitimately-named file <c>foo</c> instead of the orphan <c>foo.</c>.
    /// Cloud providers (OneDrive/GDrive are Linux-backed) happily store
    /// trailing-dot / trailing-space filenames, so they can appear in a
    /// listing even though Windows refuses to create them locally.
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

    // ---- GDrive blob listing / deletion ----

    private async Task<ListBlobsResult> ListGDriveAppBlobsAsync(
        string tokenPath, string accountId, string appId, CancellationToken cancel)
    {
        var token = await GetGDriveAccessTokenAsync(tokenPath, cancel);
        if (token == null)
            return new ListBlobsResult(Array.Empty<string>(), false,
                "Failed to get Google Drive access token. Re-authenticate in Cloud Provider settings.");

        // Walk CloudRedirect -> {accountId} -> {appId} -> blobs using the
        // checked helper so transient HTTP/parse errors do NOT masquerade as
        // "folder legitimately absent -> complete empty listing." Only a
        // genuine NotFound collapses to the empty-complete shortcut.
        var (blobsId, walkErr) = await ResolveGDriveBlobsFolderAsync(token, accountId, appId, cancel);
        if (walkErr != null)
            return new ListBlobsResult(Array.Empty<string>(), false, walkErr);
        if (blobsId == null)
            return new ListBlobsResult(Array.Empty<string>(), true, null);

        return await ListGDriveBlobsFolderChecked(token, blobsId, cancel);
    }

    /// <summary>
    /// Paginate through a GDrive folder's direct children, collecting file
    /// names (subfolders are skipped). Returns Complete=false on any HTTP
    /// error, transport exception, or malformed response, so callers that
    /// intend to make destructive decisions can refuse to proceed.
    /// </summary>
    private async Task<ListBlobsResult> ListGDriveBlobsFolderChecked(
        string token, string folderId, CancellationToken cancel)
    {
        var names = new List<string>();
        string? pageToken = null;
        // Defensive cap against a non-changing / cyclic pageToken. 10k pages
        // at 1000 items == 10M files, far beyond any realistic per-app blob
        // count; tripping this cap means the API is misbehaving and we must
        // surface it rather than loop until process exit.
        const int kMaxPages = 10_000;
        int pages = 0;

        do
        {
            cancel.ThrowIfCancellationRequested();
            if (++pages > kMaxPages)
                return new ListBlobsResult(names, false, "GDrive list exceeded pagination safety cap");
            var query = $"'{folderId}' in parents and trashed=false";
            var url = $"https://www.googleapis.com/drive/v3/files?q={Uri.EscapeDataString(query)}" +
                      "&fields=nextPageToken,files(name,mimeType)&pageSize=1000";
            if (pageToken != null)
                url += $"&pageToken={Uri.EscapeDataString(pageToken)}";

            var req = new HttpRequestMessage(HttpMethod.Get, url);
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

            HttpResponseMessage resp;
            try
            {
                resp = await _http.SendAsync(req, cancel);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                return new ListBlobsResult(names, false, $"GDrive list transport error: {ex.Message}");
            }

            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(cancel);
                return new ListBlobsResult(names, false,
                    $"GDrive list failed: HTTP {(int)resp.StatusCode}: {body}");
            }

            string json;
            try
            {
                json = await resp.Content.ReadAsStringAsync(cancel);
            }
            catch (Exception ex)
            {
                return new ListBlobsResult(names, false, $"GDrive list read error: {ex.Message}");
            }

            JsonDocument doc;
            try { doc = JsonDocument.Parse(json); }
            catch (Exception ex)
            {
                return new ListBlobsResult(names, false, $"GDrive list parse error: {ex.Message}");
            }

            using (doc)
            {
                if (doc.RootElement.TryGetProperty("files", out var files))
                {
                    foreach (var file in files.EnumerateArray())
                    {
                        var mime = file.TryGetProperty("mimeType", out var mt) ? mt.GetString() : "";
                        if (mime == "application/vnd.google-apps.folder") continue;
                        var name = file.TryGetProperty("name", out var n) ? n.GetString() : null;
                        if (!string.IsNullOrEmpty(name)) names.Add(name);
                    }
                }

                pageToken = doc.RootElement.TryGetProperty("nextPageToken", out var npt)
                    ? npt.GetString() : null;
            }
        } while (!string.IsNullOrEmpty(pageToken));

        return new ListBlobsResult(names, true, null);
    }

    private async Task<DeleteBlobsResult> DeleteGDriveAppBlobsAsync(
        string tokenPath, string accountId, string appId,
        IReadOnlyCollection<string> blobFilenames, CancellationToken cancel)
    {
        var token = await GetGDriveAccessTokenAsync(tokenPath, cancel);
        if (token == null)
            return new DeleteBlobsResult(0, blobFilenames.Count, blobFilenames.ToList(),
                "Failed to get Google Drive access token. Re-authenticate in Cloud Provider settings.");

        // Same resolver used by the listing path: distinguishes "folder
        // genuinely absent" from "couldn't verify due to HTTP/parse error."
        // If any step errors we refuse to claim silent success -- each
        // requested filename is reported as failed so the user sees the
        // actual outcome instead of a bogus "deleted=0 failed=0" toast.
        var (blobsId, walkErr) = await ResolveGDriveBlobsFolderAsync(token, accountId, appId, cancel);
        if (walkErr != null)
            return new DeleteBlobsResult(0, blobFilenames.Count, blobFilenames.ToList(), walkErr);
        if (blobsId == null)
        {
            // Folder truly absent -- every requested filename is already
            // gone on the cloud, which is idempotent success per the
            // per-file contract in FindGDriveBlobFileChecked.
            return new DeleteBlobsResult(blobFilenames.Count, 0, Array.Empty<string>(), null);
        }

        int deleted = 0, failed = 0;
        var failedNames = new List<string>();

        foreach (var filename in blobFilenames)
        {
            cancel.ThrowIfCancellationRequested();
            // Distinguish three outcomes from the lookup:
            //   FoundId(id)  - delete and report result accordingly
            //   NotFound     - idempotent success (blob legitimately absent)
            //   LookupFailed - transient HTTP / parse error; report as failed
            //                  so we don't silently claim to have deleted
            //                  something we couldn't even verify was gone.
            var lookup = await FindGDriveBlobFileChecked(token, filename, blobsId, cancel);
            switch (lookup.Kind)
            {
                case GDriveLookupKind.Found:
                {
                    var (ok, _, _) = await DeleteGDriveItem(token, lookup.FileId!, cancel);
                    if (ok) deleted++;
                    else { failed++; failedNames.Add(filename); }
                    break;
                }
                case GDriveLookupKind.NotFound:
                    deleted++; // genuinely absent on cloud -- idempotent
                    break;
                case GDriveLookupKind.Error:
                default:
                    failed++; failedNames.Add(filename);
                    break;
            }
        }

        string? err = failed > 0 ? $"{failed} of {blobFilenames.Count} file(s) could not be deleted." : null;
        return new DeleteBlobsResult(deleted, failed, failedNames, err);
    }

    /// <summary>
    /// Distinguishes the three GDrive lookup outcomes so the prune path can
    /// tell a transient HTTP/parse error apart from a genuine "not found."
    /// The pre-existing <see cref="FindGDriveFolder"/> collapses both into
    /// null, which was acceptable for its single destructive "delete app
    /// data" caller but is not safe for per-blob prune where silently
    /// conflating "I couldn't check" with "it's already gone" would let us
    /// lie to the user about what was deleted.
    /// </summary>
    private enum GDriveLookupKind { Found, NotFound, Error }
    private record struct GDriveLookupResult(GDriveLookupKind Kind, string? FileId);

    /// <summary>
    /// Walk CloudRedirect -> {accountId} -> {appId} -> blobs using the
    /// checked folder primitive. Returns a tuple:
    ///   (blobsId: string, err: null)  on success, blobsId may be null to
    ///                                 signal "folder chain genuinely absent
    ///                                 on cloud -- idempotent empty state."
    ///   (blobsId: null,   err: string) on any transient HTTP/parse failure,
    ///                                 so the caller can refuse to treat a
    ///                                 lookup gap as "nothing to do."
    /// This is the single chokepoint for distinguishing "legitimately empty"
    /// from "cannot verify" across both list and delete paths.
    /// </summary>
    private async Task<(string? BlobsId, string? Error)> ResolveGDriveBlobsFolderAsync(
        string token, string accountId, string appId, CancellationToken cancel)
    {
        var rootStep = await FindGDriveFolderChecked(token, "CloudRedirect", "root", cancel);
        if (rootStep.Kind == GDriveLookupKind.Error)
            return (null, "GDrive folder walk failed at 'CloudRedirect' (lookup error; see log)");
        if (rootStep.Kind == GDriveLookupKind.NotFound) return (null, null);

        var acctStep = await FindGDriveFolderChecked(token, accountId, rootStep.FileId!, cancel);
        if (acctStep.Kind == GDriveLookupKind.Error)
            return (null, $"GDrive folder walk failed at '{accountId}' (lookup error; see log)");
        if (acctStep.Kind == GDriveLookupKind.NotFound) return (null, null);

        var appStep = await FindGDriveFolderChecked(token, appId, acctStep.FileId!, cancel);
        if (appStep.Kind == GDriveLookupKind.Error)
            return (null, $"GDrive folder walk failed at '{appId}' (lookup error; see log)");
        if (appStep.Kind == GDriveLookupKind.NotFound) return (null, null);

        var blobsStep = await FindGDriveFolderChecked(token, "blobs", appStep.FileId!, cancel);
        if (blobsStep.Kind == GDriveLookupKind.Error)
            return (null, "GDrive folder walk failed at 'blobs' (lookup error; see log)");
        if (blobsStep.Kind == GDriveLookupKind.NotFound) return (null, null);

        return (blobsStep.FileId, null);
    }

    /// <summary>
    /// Checked counterpart to <see cref="FindGDriveFolder"/> &#8212; resolves a
    /// subfolder by exact name with full Found / NotFound / Error reporting.
    /// Mirrors the discipline of <see cref="FindGDriveBlobFileChecked"/>.
    /// The null-returning <see cref="FindGDriveFolder"/> is retained for its
    /// single destructive "delete app data" caller where "folder absent" and
    /// "lookup failed" are both acceptable (the caller reports best-effort
    /// and exits); the prune/list path cannot tolerate that ambiguity.
    /// </summary>
    private async Task<GDriveLookupResult> FindGDriveFolderChecked(
        string token, string name, string parentId, CancellationToken cancel)
    {
        var escapedName = name.Replace("\\", "\\\\").Replace("'", "\\'");
        var query = $"name='{escapedName}' and '{parentId}' in parents " +
                    "and mimeType='application/vnd.google-apps.folder' and trashed=false";
        var url = $"https://www.googleapis.com/drive/v3/files?q={Uri.EscapeDataString(query)}" +
                  "&fields=files(id,name)&orderBy=createdTime&pageSize=10";

        var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        HttpResponseMessage resp;
        try { resp = await _http.SendAsync(req, cancel); }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _log?.Invoke($"GDrive FindFolderChecked '{name}' in {parentId} transport error: {ex.Message}");
            return new GDriveLookupResult(GDriveLookupKind.Error, null);
        }

        if (!resp.IsSuccessStatusCode)
        {
            _log?.Invoke($"GDrive FindFolderChecked '{name}' in {parentId} HTTP {(int)resp.StatusCode}");
            return new GDriveLookupResult(GDriveLookupKind.Error, null);
        }

        string json;
        try { json = await resp.Content.ReadAsStringAsync(cancel); }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _log?.Invoke($"GDrive FindFolderChecked '{name}' read error: {ex.Message}");
            return new GDriveLookupResult(GDriveLookupKind.Error, null);
        }

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("files", out var files) ||
                files.ValueKind != JsonValueKind.Array)
            {
                _log?.Invoke($"GDrive FindFolderChecked '{name}' response missing 'files' array");
                return new GDriveLookupResult(GDriveLookupKind.Error, null);
            }
            if (files.GetArrayLength() == 0)
                return new GDriveLookupResult(GDriveLookupKind.NotFound, null);
            var id = files[0].GetProperty("id").GetString();
            if (string.IsNullOrEmpty(id))
                return new GDriveLookupResult(GDriveLookupKind.Error, null);
            return new GDriveLookupResult(GDriveLookupKind.Found, id);
        }
        catch (Exception ex)
        {
            _log?.Invoke($"GDrive FindFolderChecked '{name}' parse error: {ex.Message}");
            return new GDriveLookupResult(GDriveLookupKind.Error, null);
        }
    }

    /// <summary>
    /// Find a non-folder child of a GDrive folder by exact name. Returns
    /// Found with the file id, NotFound if the query ran to conclusion with
    /// zero results, or Error on any HTTP/parse failure. Escapes apostrophes
    /// and backslashes in the name to keep the v3 query language safe.
    ///
    /// Limitation: if multiple files exist with the exact same name and
    /// parent (GDrive permits this), only the first match is deleted per
    /// prune run. The caller dedups by name, so duplicates are resolved
    /// monotonically across successive scan/prune cycles rather than all
    /// at once. This is acceptable for a prune UX because each run makes
    /// progress and the numeric count stays accurate per-run; full
    /// enumeration would require paginating the query and deleting all
    /// ids, which we can add if duplicate names become a real workflow.
    /// </summary>
    private async Task<GDriveLookupResult> FindGDriveBlobFileChecked(
        string token, string name, string parentId, CancellationToken cancel)
    {
        var escapedName = name.Replace("\\", "\\\\").Replace("'", "\\'");
        var query = $"name='{escapedName}' and '{parentId}' in parents " +
                    "and mimeType!='application/vnd.google-apps.folder' and trashed=false";
        var url = $"https://www.googleapis.com/drive/v3/files?q={Uri.EscapeDataString(query)}" +
                  "&fields=files(id,name)&pageSize=10";

        var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        HttpResponseMessage resp;
        try { resp = await _http.SendAsync(req, cancel); }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _log?.Invoke($"GDrive FindBlob '{name}' transport error: {ex.Message}");
            return new GDriveLookupResult(GDriveLookupKind.Error, null);
        }

        if (!resp.IsSuccessStatusCode)
        {
            // Transient 5xx or permission 403/401 are all "cannot verify"
            // states -- NOT "file is absent." Return Error so the caller
            // can count this as a failed delete, not a spurious success.
            _log?.Invoke($"GDrive FindBlob '{name}' returned HTTP {(int)resp.StatusCode}; treating as lookup error");
            return new GDriveLookupResult(GDriveLookupKind.Error, null);
        }

        string json;
        try { json = await resp.Content.ReadAsStringAsync(cancel); }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _log?.Invoke($"GDrive FindBlob '{name}' read error: {ex.Message}");
            return new GDriveLookupResult(GDriveLookupKind.Error, null);
        }

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("files", out var files) ||
                files.ValueKind != JsonValueKind.Array)
            {
                // Malformed response -- the endpoint should always return a
                // "files" array on 200. Treat as lookup error, not "not
                // found," so we fail the delete instead of lying.
                _log?.Invoke($"GDrive FindBlob '{name}' response missing 'files' array");
                return new GDriveLookupResult(GDriveLookupKind.Error, null);
            }
            if (files.GetArrayLength() == 0)
                return new GDriveLookupResult(GDriveLookupKind.NotFound, null);
            var id = files[0].GetProperty("id").GetString();
            if (string.IsNullOrEmpty(id))
                return new GDriveLookupResult(GDriveLookupKind.Error, null);
            return new GDriveLookupResult(GDriveLookupKind.Found, id);
        }
        catch (Exception ex)
        {
            _log?.Invoke($"GDrive FindBlob '{name}' parse error: {ex.Message}");
            return new GDriveLookupResult(GDriveLookupKind.Error, null);
        }
    }

    // ---- OneDrive blob listing / deletion ----

    private async Task<ListBlobsResult> ListOneDriveAppBlobsAsync(
        string tokenPath, string accountId, string appId, CancellationToken cancel)
    {
        var token = await GetOneDriveAccessTokenAsync(tokenPath, cancel);
        if (token == null)
            return new ListBlobsResult(Array.Empty<string>(), false,
                "Failed to get OneDrive access token. Re-authenticate in Cloud Provider settings.");

        var folderPath = $"CloudRedirect/{accountId}/{appId}/blobs";
        var encoded = string.Join("/", folderPath.Split('/').Select(Uri.EscapeDataString));
        string? nextUrl = $"https://graph.microsoft.com/v1.0/me/drive/root:/{encoded}:/children?$top=200";

        var names = new List<string>();
        // Defensive cap: a non-changing / cyclic @odata.nextLink would spin
        // this loop forever holding the shared HttpClient. 10k pages at 200
        // items == 2M blobs, which is far beyond any realistic per-app count.
        const int kMaxPages = 10_000;
        int pages = 0;
        while (!string.IsNullOrEmpty(nextUrl))
        {
            cancel.ThrowIfCancellationRequested();
            if (++pages > kMaxPages)
                return new ListBlobsResult(names, false, "OneDrive list exceeded pagination safety cap");

            var req = new HttpRequestMessage(HttpMethod.Get, nextUrl);
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

            HttpResponseMessage resp;
            try { resp = await _http.SendAsync(req, cancel); }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                return new ListBlobsResult(names, false, $"OneDrive list transport error: {ex.Message}");
            }

            // NotFound is a trivially-complete empty listing ONLY on the
            // very first request. Mid-pagination NotFound means an already-
            // accumulated partial listing would be silently discarded and
            // reported as Complete=true, which is a completeness-contract
            // violation -- downstream orphan computation would run against
            // an empty cloud set and the user would see "nothing to prune"
            // when reality is unknown.
            if (resp.StatusCode == HttpStatusCode.NotFound)
            {
                if (names.Count == 0)
                    return new ListBlobsResult(Array.Empty<string>(), true, null);
                return new ListBlobsResult(names, false,
                    "OneDrive list returned NotFound mid-pagination; partial result cannot be treated as complete");
            }

            if (!resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(cancel);
                return new ListBlobsResult(names, false,
                    $"OneDrive list failed: HTTP {(int)resp.StatusCode}: {body}");
            }

            string json;
            try { json = await resp.Content.ReadAsStringAsync(cancel); }
            catch (Exception ex)
            {
                return new ListBlobsResult(names, false, $"OneDrive list read error: {ex.Message}");
            }

            JsonDocument doc;
            try { doc = JsonDocument.Parse(json); }
            catch (Exception ex)
            {
                return new ListBlobsResult(names, false, $"OneDrive list parse error: {ex.Message}");
            }

            using (doc)
            {
                if (doc.RootElement.TryGetProperty("value", out var items))
                {
                    foreach (var item in items.EnumerateArray())
                    {
                        if (item.TryGetProperty("folder", out _)) continue; // skip subfolders
                        var name = item.TryGetProperty("name", out var n) ? n.GetString() : null;
                        if (!string.IsNullOrEmpty(name)) names.Add(name);
                    }
                }

                nextUrl = doc.RootElement.TryGetProperty("@odata.nextLink", out var nl)
                    ? nl.GetString() : null;
            }
        }

        return new ListBlobsResult(names, true, null);
    }

    private async Task<DeleteBlobsResult> DeleteOneDriveAppBlobsAsync(
        string tokenPath, string accountId, string appId,
        IReadOnlyCollection<string> blobFilenames, CancellationToken cancel)
    {
        var token = await GetOneDriveAccessTokenAsync(tokenPath, cancel);
        if (token == null)
            return new DeleteBlobsResult(0, blobFilenames.Count, blobFilenames.ToList(),
                "Failed to get OneDrive access token. Re-authenticate in Cloud Provider settings.");

        int deleted = 0, failed = 0;
        var failedNames = new List<string>();

        foreach (var filename in blobFilenames)
        {
            cancel.ThrowIfCancellationRequested();
            var path = $"CloudRedirect/{accountId}/{appId}/blobs/{filename}";
            var encoded = string.Join("/", path.Split('/').Select(Uri.EscapeDataString));
            var req = new HttpRequestMessage(HttpMethod.Delete,
                $"https://graph.microsoft.com/v1.0/me/drive/root:/{encoded}:");
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

            HttpResponseMessage resp;
            try { resp = await _http.SendAsync(req, cancel); }
            catch (OperationCanceledException) { throw; } // propagate user cancel / timeout cleanly
            catch { failed++; failedNames.Add(filename); continue; }

            // NotFound == already-deleted (idempotent).
            if (resp.IsSuccessStatusCode || resp.StatusCode == HttpStatusCode.NotFound)
                deleted++;
            else { failed++; failedNames.Add(filename); }
        }

        string? err = failed > 0 ? $"{failed} of {blobFilenames.Count} file(s) could not be deleted." : null;
        return new DeleteBlobsResult(deleted, failed, failedNames, err);
    }

    // ---- Folder-provider blob listing / deletion ----

    private ListBlobsResult ListFolderAppBlobs(string syncPath, string accountId, string appId)
    {
        var blobsDir = Path.Combine(syncPath, accountId, appId, "blobs");
        if (!Directory.Exists(blobsDir))
            return new ListBlobsResult(Array.Empty<string>(), true, null);

        // TopDirectoryOnly: the native layout does not create subfolders
        // under blobs/, so anything deeper is foreign content we refuse to
        // touch from the prune path.
        var files = Directory.GetFiles(blobsDir, "*", SearchOption.TopDirectoryOnly);
        var names = new List<string>(files.Length);
        foreach (var f in files)
        {
            var n = Path.GetFileName(f);
            if (!string.IsNullOrEmpty(n)) names.Add(n);
        }
        return new ListBlobsResult(names, true, null);
    }

    private DeleteBlobsResult DeleteFolderAppBlobs(
        string syncPath, string accountId, string appId, IReadOnlyCollection<string> blobFilenames)
    {
        var blobsDir = Path.Combine(syncPath, accountId, appId, "blobs");
        // If the blobs directory is missing entirely, we cannot tell apart
        // "legitimately nothing to delete" from "sync root is unmounted /
        // misconfigured / offline." Unlike cloud providers (which auto-
        // create folders on write), the folder provider persists this
        // directory the moment anything has been cached, so a missing
        // folder after ScanAsync claimed orphans is a user-visible
        // misconfiguration. Report as failed so the UI surfaces it
        // instead of announcing a phantom success.
        if (!Directory.Exists(blobsDir))
        {
            return new DeleteBlobsResult(
                0, blobFilenames.Count, blobFilenames.ToList(),
                $"Blobs directory not found: {blobsDir}. Sync target may be offline or misconfigured.");
        }
        // Normalize WITH trailing separator so the StartsWith check cannot
        // match a sibling prefix ("blobsDir" would spuriously match
        // "blobsDir_evil\x" via string prefix). Path.GetFullPath does not
        // append a trailing separator, so add one explicitly.
        var blobsDirFull = Path.GetFullPath(blobsDir);
        if (!blobsDirFull.EndsWith(Path.DirectorySeparatorChar) &&
            !blobsDirFull.EndsWith(Path.AltDirectorySeparatorChar))
        {
            blobsDirFull += Path.DirectorySeparatorChar;
        }
        int deleted = 0, failed = 0;
        var failedNames = new List<string>();

        foreach (var filename in blobFilenames)
        {
            // Belt-and-suspenders: IsUnsafeBlobName already rejected path
            // separators and trailing-dot/space, but re-verify the combined
            // path is under blobsDir before touching disk. Defends against
            // edge cases where the runtime's path canonicalization differs
            // from our string check.
            string path;
            try
            {
                path = Path.GetFullPath(Path.Combine(blobsDir, filename));
            }
            catch
            {
                failed++; failedNames.Add(filename); continue;
            }
            if (!path.StartsWith(blobsDirFull, StringComparison.OrdinalIgnoreCase))
            {
                failed++; failedNames.Add(filename); continue;
            }

            try
            {
                if (File.Exists(path)) File.Delete(path);
                deleted++;
            }
            catch
            {
                failed++; failedNames.Add(filename);
            }
        }

        string? err = failed > 0 ? $"{failed} of {blobFilenames.Count} file(s) could not be deleted." : null;
        return new DeleteBlobsResult(deleted, failed, failedNames, err);
    }

    public void Dispose()
    {
        _http.Dispose();
    }
}
