using System;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Threading;
using Wpf.Ui.Appearance;

namespace CloudRedirect;

public partial class App : System.Windows.Application
{
    protected override void OnStartup(System.Windows.StartupEventArgs e)
    {
        ApplyLanguagePreference();
        base.OnStartup(e);
        ApplicationThemeManager.Apply(ApplicationTheme.Dark);
    }

    /// <summary>
    /// Reads the language preference from settings.json and sets the UI culture
    /// before any XAML is loaded. Falls back to system locale if missing or "system".
    /// </summary>
    private static void ApplyLanguagePreference()
    {
        try
        {
            var configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "CloudRedirect");
            var settingsPath = Path.Combine(configDir, "settings.json");

            if (!File.Exists(settingsPath)) return;

            var json = File.ReadAllText(settingsPath);
            using var doc = JsonDocument.Parse(json);

            if (!doc.RootElement.TryGetProperty("language", out var langProp))
                return;

            var lang = langProp.GetString();
            if (string.IsNullOrEmpty(lang) || lang == "system")
                return;

            var culture = new CultureInfo(lang);
            Thread.CurrentThread.CurrentUICulture = culture;
            CultureInfo.DefaultThreadCurrentUICulture = culture;
        }
        catch
        {
            // If settings.json is missing, malformed, or the culture code is invalid,
            // silently fall back to the system default.
        }
    }
}
