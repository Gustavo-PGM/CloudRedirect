using System;
using System.IO;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Wpf.Ui.Appearance;
using Wpf.Ui.Controls;
using TextBlock = System.Windows.Controls.TextBlock;

namespace CloudRedirect;

public partial class MainWindow : FluentWindow
{
    private Services.AppUpdater.CheckResult? _pendingUpdate;

    public MainWindow()
    {
        InitializeComponent();

        Loaded += async (_, _) =>
        {
            try
            {
                SystemThemeWatcher.Watch(this);

                // Fire-and-forget: auto-update check (non-blocking)
                _ = CheckForAutoUpdateAsync();

                bool needsSetup = await Task.Run(() => NeedsSetup());
                if (needsSetup)
                    RootNavigation.Navigate(typeof(Pages.SetupPage));
                else
                    RootNavigation.Navigate(typeof(Pages.DashboardPage));
            }
            catch { }
        };
    }

    /// <summary>
    /// Returns true if the DLL isn't deployed or config.json doesn't exist yet.
    /// </summary>
    private static bool NeedsSetup()
    {
        var steamPath = Services.SteamDetector.FindSteamPath();
        if (steamPath == null) return true;

        if (!File.Exists(Path.Combine(steamPath, "cloud_redirect.dll")))
            return true;

        var configPath = Services.SteamDetector.GetConfigFilePath();
        if (!File.Exists(configPath))
            return true;

        return false;
    }

    /// <summary>
    /// Checks GitHub for a newer version. If found, shows an inline banner
    /// with changelog and Update/Skip buttons.
    /// </summary>
    private async Task CheckForAutoUpdateAsync()
    {
        try
        {
            var result = await Services.AppUpdater.CheckAsync();
            if (result == null || !result.UpdateAvailable || result.DownloadUrl == null)
                return;

            _pendingUpdate = result;
            var versionStr = result.TagName?.TrimStart('v') ?? result.TagName ?? "unknown";
            var body = result.Body?.Trim() ?? "";

            UpdateBannerTitle.Text = $"Update available -- v{versionStr}";
            UpdateBannerStatus.Text = "A new version of CloudRedirect is ready to install.";

            if (!string.IsNullOrEmpty(body))
            {
                UpdateChangelogText.Text = body;
                UpdateChangelogScroll.Visibility = Visibility.Visible;
            }

            UpdateBanner.Visibility = Visibility.Visible;
        }
        catch
        {
            // Auto-update check failures are non-fatal
        }
    }

    private async void UpdateNow_Click(object sender, RoutedEventArgs e)
    {
        if (_pendingUpdate?.DownloadUrl == null) return;

        var versionStr = _pendingUpdate.TagName?.TrimStart('v') ?? "unknown";

        // Switch banner to download mode
        UpdateNowButton.Visibility = Visibility.Collapsed;
        UpdateSkipButton.Visibility = Visibility.Collapsed;
        UpdateChangelogScroll.Visibility = Visibility.Collapsed;
        UpdateBannerStatus.Text = $"Downloading v{versionStr}...";
        UpdateProgressBar.Visibility = Visibility.Visible;
        UpdateProgressBar.IsIndeterminate = true;

        var error = await Services.AppUpdater.DownloadAndApplyAsync(
            _pendingUpdate.DownloadUrl,
            (pct, status) => Dispatcher.Invoke(() =>
            {
                UpdateBannerStatus.Text = status;
                if (pct >= 0)
                {
                    UpdateProgressBar.IsIndeterminate = false;
                    UpdateProgressBar.Value = pct;
                }
                else
                {
                    UpdateProgressBar.IsIndeterminate = true;
                }
            }));

        if (error != null)
        {
            // Show error, restore buttons so user can retry
            UpdateBannerTitle.Text = "Update failed";
            UpdateBannerStatus.Text = error;
            UpdateProgressBar.Visibility = Visibility.Collapsed;
            UpdateBanner.Background = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromArgb(0x33, 0xC4, 0x2B, 0x1C));
            UpdateBanner.BorderBrush = new System.Windows.Media.SolidColorBrush(
                System.Windows.Media.Color.FromRgb(0xC4, 0x2B, 0x1C));
            UpdateNowButton.Visibility = Visibility.Visible;
            UpdateSkipButton.Visibility = Visibility.Visible;
        }
        // If successful, the process will have exited already
    }

    private void UpdateSkip_Click(object sender, RoutedEventArgs e)
    {
        UpdateBanner.Visibility = Visibility.Collapsed;
        _pendingUpdate = null;
    }
}
