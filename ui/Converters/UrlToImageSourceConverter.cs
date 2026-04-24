using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media.Imaging;
using CloudRedirect.Services;

namespace CloudRedirect.Converters;

/// <summary>
/// Converts a string URL (Steam CDN https:// or a file:// path pointing at
/// our on-disk artwork cache) into a frozen <see cref="BitmapImage"/> for
/// declarative binding from XAML.
///
/// Behavior contract:
/// <list type="bullet">
///   <item><description>
///     Null / empty / non-string / non-URI inputs return
///     <see cref="Binding.DoNothing"/> so the target property keeps its
///     design-time or template-default value. Returning <c>null</c> would
///     clear an <see cref="System.Windows.Media.ImageBrush.ImageSource"/>
///     that was set by another trigger.
///   </description></item>
///   <item><description>
///     The URL is validated through
///     <see cref="SteamStoreClient.IsValidImageUrl"/>, which path-traversal-
///     guards <c>file://</c> inputs against the artwork cache directory.
///     Anything rejected by that gate returns <see cref="Binding.DoNothing"/>.
///   </description></item>
///   <item><description>
///     <see cref="BitmapCacheOption.OnLoad"/> is set so the image is decoded
///     immediately and the backing file handle is released. Without it a
///     <c>file://</c> URI keeps the cached JPEG locked for the lifetime of
///     the <see cref="BitmapImage"/>, which would block both the cache
///     eviction sweep in <see cref="SteamStoreClient"/> and the
///     <c>File.Move(overwrite: true)</c> that installs a refreshed asset
///     after a Steam CDN hash rotation.
///   </description></item>
///   <item><description>
///     <see cref="System.Windows.Freezable.Freeze"/> is called so the
///     produced bitmap is safe to hand off to the UI thread without
///     per-access dispatcher marshalling and can be shared across bindings.
///   </description></item>
/// </list>
///
/// This converter intentionally has no fallback image; a failed decode (bad
/// JPEG, network failure on a CDN URI, cache file deleted between validation
/// and decode) returns <see cref="Binding.DoNothing"/> and lets the caller
/// surface a template-level placeholder.
/// </summary>
public sealed class UrlToImageSourceConverter : IValueConverter
{
    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is not string url || string.IsNullOrWhiteSpace(url))
            return Binding.DoNothing;

        if (!SteamStoreClient.IsValidImageUrl(url))
            return Binding.DoNothing;

        Uri? uri;
        try
        {
            uri = new Uri(url);
        }
        catch
        {
            return Binding.DoNothing;
        }

        try
        {
            var bitmap = new BitmapImage();
            bitmap.BeginInit();
            // OnLoad decodes immediately and releases the backing file handle;
            // required so cache eviction and atomic File.Move(overwrite:true)
            // on asset hash rotation aren't blocked by an outstanding lock.
            bitmap.CacheOption = BitmapCacheOption.OnLoad;
            bitmap.UriSource = uri;
            bitmap.EndInit();
            bitmap.Freeze();
            return bitmap;
        }
        catch
        {
            return Binding.DoNothing;
        }
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
        => throw new NotSupportedException();
}
