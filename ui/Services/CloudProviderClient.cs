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

    public void Dispose()
    {
        _http.Dispose();
    }
}
