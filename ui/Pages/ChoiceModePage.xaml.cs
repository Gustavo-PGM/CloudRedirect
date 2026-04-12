using System;
using System.IO;
using System.Text.Json;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using CloudRedirect.Resources;
using CloudRedirect.Services;
using CloudRedirect.Windows;

namespace CloudRedirect.Pages;

public partial class ChoiceModePage : Page
{
    private string? _currentMode;

    public ChoiceModePage()
    {
        InitializeComponent();
        Loaded += (_, _) => RefreshState();
    }

    private void RefreshState()
    {
        _currentMode = SteamDetector.ReadModeSetting();

        if (_currentMode != null)
        {
            CurrentModeBanner.Visibility = Visibility.Visible;

            if (_currentMode == "cloud_redirect")
            {
                CurrentModeText.Text = S.Get("Choice_CurrentMode_CloudRedirect");
                CurrentModeDescription.Text = S.Get("Choice_CurrentMode_CloudRedirect_Desc");
                STFixerCard.Visibility = Visibility.Collapsed;
                CloudRedirectCard.Visibility = Visibility.Collapsed;
            }
            else
            {
                CurrentModeText.Text = S.Get("Choice_CurrentMode_STFixer");
                CurrentModeDescription.Text = S.Get("Choice_CurrentMode_STFixer_Desc");
                STFixerCard.Visibility = Visibility.Collapsed;
                CloudRedirectCard.Visibility = Visibility.Visible;
            }
        }
        else
        {
            CurrentModeBanner.Visibility = Visibility.Collapsed;
            STFixerCard.Visibility = Visibility.Visible;
            CloudRedirectCard.Visibility = Visibility.Visible;
        }
    }

    private void STFixerCard_Click(object sender, MouseButtonEventArgs e)
    {
        if (_currentMode == "cloud_redirect") return;

        SaveModeSetting("stfixer");
        SetDllCloudRedirect(false);

        var mw = Window.GetWindow(this) as MainWindow;
        mw?.ApplyMode("stfixer");
        mw?.RootNavigation.Navigate(typeof(SetupPage));
    }

    private void CloudRedirectCard_Click(object sender, MouseButtonEventArgs e)
    {
        var disclaimer = new DisclaimerWindow
        {
            Owner = Window.GetWindow(this)
        };

        var result = disclaimer.ShowDialog();

        if (result != true || !disclaimer.Accepted)
            return;

        SaveModeSetting("cloud_redirect");
        SetDllCloudRedirect(true);

        var mw = Window.GetWindow(this) as MainWindow;
        mw?.ApplyMode("cloud_redirect");
        mw?.RootNavigation.Navigate(typeof(SetupPage));
    }

    private static void SaveModeSetting(string mode)
    {
        try
        {
            var path = GetSettingsPath();
            var dir = Path.GetDirectoryName(path)!;
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            JsonElement existing = default;
            if (File.Exists(path))
            {
                try
                {
                    var oldJson = File.ReadAllText(path);
                    using var oldDoc = JsonDocument.Parse(oldJson);
                    existing = oldDoc.RootElement.Clone();
                }
                catch { }
            }

            using var ms = new MemoryStream();
            using (var writer = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = true }))
            {
                writer.WriteStartObject();
                writer.WriteString("mode", mode);

                if (existing.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in existing.EnumerateObject())
                    {
                        if (prop.Name == "mode") continue;
                        prop.WriteTo(writer);
                    }
                }

                writer.WriteEndObject();
            }

            var newJson = System.Text.Encoding.UTF8.GetString(ms.ToArray());
            FileUtils.AtomicWriteAllText(path, newJson);
        }
        catch { }
    }

    private static void SetDllCloudRedirect(bool enabled)
    {
        try
        {
            var path = SteamDetector.GetPinConfigPath();
            if (path == null) return;

            JsonElement existing = default;
            if (File.Exists(path))
            {
                try
                {
                    var oldJson = File.ReadAllText(path);
                    using var oldDoc = JsonDocument.Parse(oldJson, new JsonDocumentOptions
                    {
                        CommentHandling = JsonCommentHandling.Skip
                    });
                    existing = oldDoc.RootElement.Clone();
                }
                catch { }
            }

            using var ms = new MemoryStream();
            using (var writer = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = true }))
            {
                writer.WriteStartObject();
                writer.WriteBoolean("cloud_redirect", enabled);

                if (existing.ValueKind == JsonValueKind.Object)
                {
                    foreach (var prop in existing.EnumerateObject())
                    {
                        if (prop.Name == "cloud_redirect") continue;
                        prop.WriteTo(writer);
                    }
                }

                writer.WriteEndObject();
            }

            var dir = Path.GetDirectoryName(path)!;
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            var newJson = System.Text.Encoding.UTF8.GetString(ms.ToArray());
            FileUtils.AtomicWriteAllText(path, newJson);
        }
        catch { }
    }

    private static string GetSettingsPath()
    {
        return Path.Combine(SteamDetector.GetConfigDir(), "settings.json");
    }
}
