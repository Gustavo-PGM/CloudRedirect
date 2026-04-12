using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Controls;
using CloudRedirect.Resources;
using CloudRedirect.Services;
using Wpf.Ui.Controls;

namespace CloudRedirect.Pages;

public partial class ManifestPinningPage : Page
{
    private bool _loading;
    private readonly List<LuaApp> _apps = new();
    private readonly HashSet<uint> _pinnedApps = new();
    private readonly SteamStoreClient _storeClient = SteamStoreClient.Shared;

    public ManifestPinningPage()
    {
        InitializeComponent();
        Loaded += async (_, _) =>
        {
            _loading = true;
            try
            {
                LoadConfig();
                ScanLuaFiles();
                ApplyPinnedState();
                RefreshList();
                await ResolveAppNamesAsync();
            }
            finally
            {
                _loading = false;
            }
        };
    }

    private void LoadConfig()
    {
        try
        {
            var path = SteamDetector.GetPinConfigPath();
            if (path == null || !File.Exists(path)) return;

            var json = File.ReadAllText(path);
            using var doc = JsonDocument.Parse(json, new JsonDocumentOptions
            {
                CommentHandling = JsonCommentHandling.Skip
            });
            var root = doc.RootElement;

            if (root.TryGetProperty("manifest_pinning", out var mp) && mp.ValueKind == JsonValueKind.True)
                ManifestPinningToggle.IsChecked = true;
            if (root.TryGetProperty("auto_comment", out var ac) && ac.ValueKind == JsonValueKind.True)
                AutoCommentToggle.IsChecked = true;

            _pinnedApps.Clear();
            if (root.TryGetProperty("pinned_apps", out var arr) && arr.ValueKind == JsonValueKind.Array)
            {
                foreach (var el in arr.EnumerateArray())
                {
                    if (el.TryGetUInt32(out var appId))
                        _pinnedApps.Add(appId);
                }
            }
        }
        catch { }
    }

    private static readonly Regex ManifestIdRegex = new(
        @"setManifestid\s*\(\s*(\d+)\s*,\s*""(\d+)""",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private void ScanLuaFiles()
    {
        _apps.Clear();
        try
        {
            var steamPath = SteamDetector.FindSteamPath();
            if (steamPath == null) return;

            var luaDir = Path.Combine(steamPath, "config", "stplug-in");
            if (!Directory.Exists(luaDir)) return;

            foreach (var file in Directory.GetFiles(luaDir, "*.lua"))
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                if (!uint.TryParse(fileName, out var appId) || appId == 0) continue;

                var depots = new List<DepotEntry>();
                foreach (var line in File.ReadLines(file))
                {
                    var trimmed = line.TrimStart();
                    if (trimmed.StartsWith("--")) continue;

                    var match = ManifestIdRegex.Match(line);
                    if (!match.Success) continue;

                    if (uint.TryParse(match.Groups[1].Value, out _) &&
                        ulong.TryParse(match.Groups[2].Value, out _))
                    {
                        depots.Add(new DepotEntry
                        {
                            DepotId = match.Groups[1].Value,
                            ManifestId = match.Groups[2].Value
                        });
                    }
                }

                if (depots.Count == 0) continue;

                _apps.Add(new LuaApp
                {
                    AppId = appId,
                    DisplayName = S.Format("Pin_AppFallbackName", appId),
                    Depots = depots
                });
            }

            _apps.Sort((a, b) => a.AppId.CompareTo(b.AppId));
        }
        catch { }
    }

    private void ApplyPinnedState()
    {
        foreach (var app in _apps)
            app.IsPinned = _pinnedApps.Contains(app.AppId);
    }

    private void RefreshList()
    {
        var query = SearchBox?.Text?.Trim() ?? "";
        var filtered = string.IsNullOrEmpty(query)
            ? _apps.ToList()
            : _apps.Where(a =>
                a.DisplayName.Contains(query, StringComparison.OrdinalIgnoreCase) ||
                a.AppId.ToString().Contains(query, StringComparison.OrdinalIgnoreCase))
              .ToList();

        NoPinsText.Visibility = _apps.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        AppList.ItemsSource = null;
        AppList.ItemsSource = filtered;
    }

    private async System.Threading.Tasks.Task ResolveAppNamesAsync()
    {
        var ids = _apps.Select(a => a.AppId).Distinct().ToList();
        if (ids.Count == 0) return;

        try
        {
            var infos = await _storeClient.GetAppInfoAsync(ids);
            foreach (var app in _apps)
            {
                if (infos.TryGetValue(app.AppId, out var info))
                {
                    if (!string.IsNullOrEmpty(info.Name))
                        app.Name = info.Name;
                    app.HeaderUrl = info.HeaderUrl;
                }
            }
            RefreshList();
        }
        catch { }
    }

    private void PinToggle_Changed(object sender, RoutedEventArgs e)
    {
        if (_loading) return;
        SaveConfig();
        NotifyRestartNeeded();
    }

    private void AppPin_Changed(object sender, RoutedEventArgs e)
    {
        if (_loading) return;

        _pinnedApps.Clear();
        foreach (var app in _apps)
        {
            if (app.IsPinned)
                _pinnedApps.Add(app.AppId);
        }

        SaveConfig();
        NotifyRestartNeeded();
    }

    private void ExpandCollapse_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement { Tag: LuaApp app }) return;
        app.IsExpanded = !app.IsExpanded;
        RefreshList();
    }

    private void SearchBox_TextChanged(object sender, TextChangedEventArgs e)
    {
        RefreshList();
    }

    private void NotifyRestartNeeded()
    {
        if (Window.GetWindow(this) is MainWindow mainWindow)
            mainWindow.ShowRestartSteam();
    }

    private void SaveConfig()
    {
        try
        {
            var path = SteamDetector.GetPinConfigPath();
            if (path == null) return;

            // Read existing cloud_redirect value to preserve it
            bool cloudRedirect = true;
            if (File.Exists(path))
            {
                try
                {
                    var existing = File.ReadAllText(path);
                    using var doc = JsonDocument.Parse(existing, new JsonDocumentOptions
                    {
                        CommentHandling = JsonCommentHandling.Skip
                    });
                    if (doc.RootElement.TryGetProperty("cloud_redirect", out var cr))
                        cloudRedirect = cr.ValueKind == JsonValueKind.True;
                }
                catch { }
            }

            using var ms = new MemoryStream();
            using (var writer = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = true }))
            {
                writer.WriteStartObject();
                writer.WriteBoolean("cloud_redirect", cloudRedirect);
                writer.WriteBoolean("manifest_pinning", ManifestPinningToggle.IsChecked == true);
                writer.WriteBoolean("auto_comment", AutoCommentToggle.IsChecked == true);

                writer.WriteStartArray("pinned_apps");
                foreach (var appId in _pinnedApps.OrderBy(x => x))
                    writer.WriteNumberValue(appId);
                writer.WriteEndArray();

                writer.WriteEndObject();
            }

            var dir = Path.GetDirectoryName(path)!;
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            var json = System.Text.Encoding.UTF8.GetString(ms.ToArray());
            FileUtils.AtomicWriteAllText(path, json);
        }
        catch { }
    }

    internal class LuaApp
    {
        public uint AppId { get; set; }
        public string Name { get; set; } = "";
        public string? HeaderUrl { get; set; }
        public bool IsPinned { get; set; }
        public bool IsExpanded { get; set; }
        public List<DepotEntry> Depots { get; set; } = new();

        public string DisplayName
        {
            get => !string.IsNullOrEmpty(Name) ? Name : _displayName;
            set => _displayName = value;
        }
        private string _displayName = "";

        public string DepotSummary => $"{Depots.Count} depot{(Depots.Count != 1 ? "s" : "")}";

        public SymbolRegular ChevronSymbol =>
            IsExpanded ? SymbolRegular.ChevronDown24 : SymbolRegular.ChevronRight24;

        public Visibility DepotsVisibility =>
            IsExpanded ? Visibility.Visible : Visibility.Collapsed;
    }

    internal class DepotEntry
    {
        public string DepotId { get; set; } = "";
        public string ManifestId { get; set; } = "";
    }
}
