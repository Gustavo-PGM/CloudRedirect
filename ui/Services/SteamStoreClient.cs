using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
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
    private static readonly string CachePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "CloudRedirect", "store_cache.json");

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

        return result;
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
