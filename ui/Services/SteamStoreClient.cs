using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace CloudRedirect.Services;

// Source generator for AOT-compatible JSON serialization
[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(StoreCache))]
internal partial class StoreCacheJsonContext : JsonSerializerContext { }

/// <summary>
/// Cached entry for a single app from IStoreBrowseService/GetItems.
/// </summary>
internal class StoreAppInfo
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("headerUrl")]
    public string? HeaderUrl { get; set; }

    [JsonPropertyName("fetchedUtc")]
    public DateTime FetchedUtc { get; set; }
}

/// <summary>
/// Disk cache format.
/// </summary>
internal class StoreCache
{
    [JsonPropertyName("entries")]
    public Dictionary<uint, StoreAppInfo> Entries { get; set; } = new();
}

/// <summary>
/// Fetches app names and header images from Steam's public IStoreBrowseService/GetItems API.
/// Results are cached in memory and on disk (%APPDATA%/CloudRedirect/store_cache.json).
/// </summary>
internal sealed class SteamStoreClient : IDisposable
{
    /// <summary>
    /// Shared singleton instance. Lives for the app's lifetime -- HttpClient is designed
    /// to be long-lived and reuse connections. Avoids the IDisposable-never-called problem
    /// when WPF Pages each create their own instance.
    /// </summary>
    public static SteamStoreClient Shared { get; } = new();

    private static readonly TimeSpan DiskCacheTtl = TimeSpan.FromDays(7);
    private static readonly TimeSpan ImageCacheTtl = TimeSpan.FromDays(60);
    private const long MaxCachedImageBytes = 5 * 1024 * 1024; // 5 MB sanity cap
    private static readonly string CachePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "CloudRedirect", "store_cache.json");

    /// <summary>
    /// Where cached JPEG headers live on disk. Filenames are derived from a
    /// SHA-256 prefix of the CDN URL so that a new Steam asset hash (which
    /// is embedded in the URL) produces a new cache file and the stale one
    /// ages out via <see cref="ImageCacheTtl"/>.
    /// </summary>
    private static readonly string ImageCacheDir = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "CloudRedirect", "store_images");

    /// <summary>
    /// Known Steam CDN domains. URLs from the disk cache are validated against these
    /// before being used as image sources to prevent tampered cache entries from
    /// loading images from attacker-controlled servers.
    /// </summary>
    private static readonly string[] AllowedCdnHosts = new[]
    {
        ".steamstatic.com",
        ".steampowered.com",
        ".steamcdn-a.akamaihd.net"
    };

    private readonly HttpClient _http;
    private readonly ConcurrentDictionary<uint, StoreAppInfo> _mem = new();
    private volatile bool _diskLoaded;
    private readonly SemaphoreSlim _diskLock = new(1, 1);

    /// <summary>
    /// Per-URL in-flight download dedup. Four WPF pages call
    /// <see cref="GetAppInfoAsync"/> on the same shared singleton at startup; without
    /// this table each launch would download the same JPEG N times. The task is
    /// stored (not the result) so every concurrent caller awaits the same download.
    /// </summary>
    private readonly ConcurrentDictionary<string, Task> _inFlightDownloads = new();

    public SteamStoreClient()
    {
        _http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
    }

    /// <summary>
    /// Returns true if the URL is a valid HTTPS URL pointing to a known Steam CDN domain.
    /// Used to validate header image URLs loaded from the disk cache before passing them
    /// to BitmapImage.
    /// </summary>
    public static bool IsValidSteamCdnUrl(string? url)
    {
        if (string.IsNullOrEmpty(url)) return false;
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri)) return false;
        if (uri.Scheme != "https") return false;

        var host = uri.Host;
        foreach (var allowed in AllowedCdnHosts)
        {
            if (host.EndsWith(allowed, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Returns true if <paramref name="url"/> is a file:// URI pointing to a path inside
    /// <see cref="ImageCacheDir"/>. The full path is normalized and compared against the
    /// cache dir (plus a trailing separator) to prevent crafted file:// URIs from escaping
    /// the cache root via traversal (e.g. <c>file:///.../store_images/../secret</c>).
    /// </summary>
    public static bool IsValidCachedImagePath(string? url)
    {
        if (string.IsNullOrEmpty(url)) return false;
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri)) return false;
        if (uri.Scheme != "file") return false;

        string normalizedLocal;
        string normalizedCache;
        try
        {
            normalizedLocal = Path.GetFullPath(uri.LocalPath);
            normalizedCache = Path.GetFullPath(ImageCacheDir);
        }
        catch
        {
            return false;
        }

        var prefix = normalizedCache.EndsWith(Path.DirectorySeparatorChar)
            ? normalizedCache
            : normalizedCache + Path.DirectorySeparatorChar;

        return normalizedLocal.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Accepts either a Steam CDN URL or a file:// URI under the local image cache.
    /// Callers that previously used <see cref="IsValidSteamCdnUrl"/> should switch to this
    /// so that rewritten cached-image URLs (see <see cref="GetAppInfoAsync"/>) also pass.
    /// </summary>
    public static bool IsValidImageUrl(string? url)
        => IsValidSteamCdnUrl(url) || IsValidCachedImagePath(url);

    /// <summary>
    /// Computes the on-disk cache path for a given CDN URL. Uses a SHA-256 prefix of the
    /// URL as the filename so that a new Steam asset hash (embedded in the URL path)
    /// produces a new cache file; old files age out via <see cref="ImageCacheTtl"/>.
    /// </summary>
    private static string GetCachedImagePath(string cdnUrl)
    {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(cdnUrl), hash);
        var hex = Convert.ToHexString(hash[..8]); // 16 hex chars is plenty for uniqueness
        return Path.Combine(ImageCacheDir, $"{hex}.jpg");
    }

    /// <summary>
    /// Look up names and header images for the given app IDs.
    /// Returns a dictionary keyed by app ID. Missing/failed apps are omitted.
    /// </summary>
    public async Task<Dictionary<uint, StoreAppInfo>> GetAppInfoAsync(IReadOnlyList<uint> appIds)
    {
        if (appIds.Count == 0)
            return new Dictionary<uint, StoreAppInfo>();

        // Load disk cache once
        await EnsureDiskCacheLoaded();

        var result = new Dictionary<uint, StoreAppInfo>();
        var toFetch = new List<uint>();

        foreach (var id in appIds)
        {
            if (_mem.TryGetValue(id, out var cached) && DateTime.UtcNow - cached.FetchedUtc < DiskCacheTtl)
                result[id] = cached;
            else
                toFetch.Add(id);
        }

        if (toFetch.Count > 0)
        {
            var fetched = await FetchFromApiAsync(toFetch);
            foreach (var (id, info) in fetched)
            {
                _mem[id] = info;
                result[id] = info;
            }

            // Persist to disk (fire and forget -- not critical)
            _ = SaveDiskCacheAsync();
        }

        // Post-process: prefer a locally cached JPEG over the CDN URL. If the cache
        // file doesn't exist yet, kick off a background download so the next launch
        // can serve it from disk. The in-memory StoreAppInfo is *not* mutated, so the
        // persisted JSON keeps the original CDN URL -- cache invalidation happens
        // naturally because a new Steam asset hash produces a new cache filename.
        //
        // Keys are snapshotted because we write back into `result` inside the loop;
        // Dictionary<,> does not currently fail-fast on indexer replacement of an
        // existing key, but that's an implementation detail we should not depend on.
        foreach (var key in result.Keys.ToArray())
        {
            var info = result[key];
            if (!IsValidSteamCdnUrl(info.HeaderUrl)) continue;

            var localPath = GetCachedImagePath(info.HeaderUrl!);
            if (File.Exists(localPath))
            {
                var rewritten = new StoreAppInfo
                {
                    Name = info.Name,
                    HeaderUrl = new Uri(localPath).AbsoluteUri,
                    FetchedUtc = info.FetchedUtc
                };
                result[key] = rewritten;
            }
            else
            {
                // GetOrAdd ensures concurrent callers with the same URL share one
                // download task instead of each racing to write the same .tmp file.
                _ = _inFlightDownloads.GetOrAdd(localPath, lp =>
                    DownloadImageToCacheAsync(info.HeaderUrl!, lp)
                        .ContinueWith(t =>
                        {
                            _inFlightDownloads.TryRemove(lp, out _);
                        }, TaskScheduler.Default));
            }
        }

        return result;
    }

    /// <summary>
    /// Downloads a Steam CDN image to the on-disk cache. Uses
    /// <see cref="HttpCompletionOption.ResponseHeadersRead"/> + streaming read with
    /// a hard <see cref="MaxCachedImageBytes"/> cap so a misbehaving or compromised
    /// CDN cannot exhaust memory on the UI process. A GUID-suffixed <c>.tmp</c>
    /// sidecar prevents two concurrent downloads of the same URL from corrupting
    /// each other's partial writes, and <see cref="File.Move(string,string,bool)"/>
    /// gives an atomic cache install (consumers never see a partial file). All
    /// failures are swallowed -- the next call will just re-fetch from the CDN.
    /// </summary>
    private async Task DownloadImageToCacheAsync(string cdnUrl, string localPath)
    {
        if (!IsValidSteamCdnUrl(cdnUrl)) return;

        string? tmp = null;
        try
        {
            Directory.CreateDirectory(ImageCacheDir);

            // Race: another caller may have created the file since the existence
            // check in GetAppInfoAsync. Recheck to avoid the redundant download.
            if (File.Exists(localPath)) return;

            // ResponseHeadersRead returns as soon as the headers are available so
            // we can reject the oversize case *before* buffering the body. Without
            // this the default ResponseContentRead mode would pull the entire body
            // into HttpResponseMessage before MaxCachedImageBytes is ever checked.
            using var resp = await _http.GetAsync(cdnUrl, HttpCompletionOption.ResponseHeadersRead);
            if (!resp.IsSuccessStatusCode) return;

            // Reject absurd payloads: Steam headers are ~15-60 KB; anything over
            // MaxCachedImageBytes is almost certainly not a header image. When a
            // Content-Length header is present, fail early.
            if (resp.Content.Headers.ContentLength is long declared && declared > MaxCachedImageBytes)
                return;

            // GUID-suffixed tmp prevents two concurrent downloaders of the same URL
            // (rare with _inFlightDownloads dedup, but possible if a stale task
            // entry was removed mid-flight) from clobbering each other's partial
            // writes.
            tmp = $"{localPath}.{Guid.NewGuid():N}.tmp";

            // Stream into disk with a running byte counter so a chunked response
            // lacking Content-Length is still bounded by MaxCachedImageBytes. The
            // FileStream uses FileShare.None so nothing else can read the half-
            // written file before the Move installs it.
            long written = 0;
            await using (var src = await resp.Content.ReadAsStreamAsync())
            await using (var dst = new FileStream(
                tmp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, useAsync: true))
            {
                var buf = new byte[81920];
                int n;
                while ((n = await src.ReadAsync(buf.AsMemory())) > 0)
                {
                    written += n;
                    if (written > MaxCachedImageBytes)
                        return; // tmp is cleaned up in finally
                    await dst.WriteAsync(buf.AsMemory(0, n));
                }
            }

            if (written == 0) return; // empty body -- treat as failure

            File.Move(tmp, localPath, overwrite: true);
            tmp = null; // don't delete the file we just moved
        }
        catch
        {
            // Non-fatal: caller will fall back to the CDN URL this session.
        }
        finally
        {
            if (tmp != null)
            {
                try { File.Delete(tmp); } catch { /* best-effort */ }
            }
        }
    }

    /// <summary>
    /// Deletes cached image files older than <see cref="ImageCacheTtl"/>, plus any
    /// <c>.tmp</c> sidecars older than 10 minutes (abandoned by a prior killed
    /// session). Bounds unbounded growth over time (old cache entries for apps the
    /// user no longer owns, or stale files left behind when a Steam asset hash
    /// changes). Enumerating with explicit patterns avoids racing an in-flight
    /// download's <c>.tmp</c> file. Run off the UI thread so a large cache (10k+
    /// files) cannot stall first-paint; safe to run unordered with other cache
    /// operations because every file-system op is per-file best-effort.
    /// </summary>
    private static void EvictStaleImages()
    {
        try
        {
            if (!Directory.Exists(ImageCacheDir)) return;
            var cutoff = DateTime.UtcNow - ImageCacheTtl;

            foreach (var file in Directory.EnumerateFiles(ImageCacheDir, "*.jpg"))
            {
                try
                {
                    if (File.GetLastWriteTimeUtc(file) < cutoff)
                        File.Delete(file);
                }
                catch { /* skip files we can't stat/delete */ }
            }

            // Separately sweep abandoned .tmp sidecars. Ten minutes is well past
            // any realistic HttpClient timeout, so only truly orphaned files (from
            // a killed session) get cleaned up -- an in-flight download is safe.
            var tmpCutoff = DateTime.UtcNow - TimeSpan.FromMinutes(10);
            foreach (var file in Directory.EnumerateFiles(ImageCacheDir, "*.tmp"))
            {
                try
                {
                    if (File.GetLastWriteTimeUtc(file) < tmpCutoff)
                        File.Delete(file);
                }
                catch { /* skip files we can't stat/delete */ }
            }
        }
        catch { /* non-fatal */ }
    }

    // ── API call ────────────────────────────────────────────────────────

    private async Task<Dictionary<uint, StoreAppInfo>> FetchFromApiAsync(List<uint> appIds)
    {
        var result = new Dictionary<uint, StoreAppInfo>();

        try
        {
            // Build the request JSON -- batch all IDs in one call
            var ids = appIds.Select(id => new { appid = id }).ToArray();
            var requestObj = new
            {
                ids,
                context = new { language = "english", country_code = "US" },
                data_request = new { include_basic_info = true, include_assets = true }
            };

            var inputJson = JsonSerializer.Serialize(requestObj);
            var encoded = Uri.EscapeDataString(inputJson);
            var url = $"https://api.steampowered.com/IStoreBrowseService/GetItems/v1?input_json={encoded}";

            var resp = await _http.GetAsync(url);
            if (!resp.IsSuccessStatusCode)
                return result;

            var body = await resp.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(body);

            if (!doc.RootElement.TryGetProperty("response", out var response))
                return result;
            if (!response.TryGetProperty("store_items", out var items))
                return result;

            foreach (var item in items.EnumerateArray())
            {
                if (!item.TryGetProperty("appid", out var appIdEl))
                    continue;

                uint appId = appIdEl.GetUInt32();

                var info = new StoreAppInfo
                {
                    FetchedUtc = DateTime.UtcNow
                };

                // Name
                if (item.TryGetProperty("name", out var nameEl))
                    info.Name = nameEl.GetString() ?? "";

                // Header image URL: assets.header can be "header.jpg" (old) or "{hash}/header.jpg" (new)
                if (item.TryGetProperty("assets", out var assets) &&
                    assets.TryGetProperty("header", out var headerEl))
                {
                    var header = headerEl.GetString();
                    if (!string.IsNullOrEmpty(header))
                        info.HeaderUrl = $"https://shared.steamstatic.com/store_item_assets/steam/apps/{appId}/{header}";
                }

                result[appId] = info;
            }
        }
        catch
        {
            // Network/parse failures are non-fatal -- we just don't get names
        }

        return result;
    }

    // ── Disk cache ──────────────────────────────────────────────────────

    private async Task EnsureDiskCacheLoaded()
    {
        if (_diskLoaded) return;

        await _diskLock.WaitAsync();
        try
        {
            if (_diskLoaded) return;

            if (File.Exists(CachePath))
            {
                var json = await File.ReadAllTextAsync(CachePath);
                var cache = JsonSerializer.Deserialize(json, StoreCacheJsonContext.Default.StoreCache);
                if (cache?.Entries != null)
                {
                    foreach (var (id, info) in cache.Entries)
                        _mem.TryAdd(id, info);
                }
            }

            // Age out stale image-cache entries on a worker thread so a large
            // cache cannot block first-paint -- every other caller of
            // GetAppInfoAsync is waiting on _diskLock right now, and eviction
            // has no ordering dependency with any of them.
            _ = Task.Run(EvictStaleImages);

            _diskLoaded = true;
        }
        catch
        {
            _diskLoaded = true; // Don't retry on corrupt cache
        }
        finally
        {
            _diskLock.Release();
        }
    }

    private async Task SaveDiskCacheAsync()
    {
        try
        {
            var cache = new StoreCache
            {
                Entries = new Dictionary<uint, StoreAppInfo>(_mem)
            };

            var dir = Path.GetDirectoryName(CachePath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            var json = JsonSerializer.Serialize(cache, StoreCacheJsonContext.Default.StoreCache);
            await File.WriteAllTextAsync(CachePath, json);
        }
        catch
        {
            // Non-fatal
        }
    }

    public void Dispose()
    {
        _http.Dispose();
        _diskLock.Dispose();
    }
}
