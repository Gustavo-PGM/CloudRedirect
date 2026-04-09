using System.Globalization;
using System.Resources;

namespace CloudRedirect.Resources;

/// <summary>
/// Simple static wrapper around <see cref="ResourceManager"/> for accessing
/// localized strings from Strings.resx.
/// </summary>
internal static class S
{
    private static readonly ResourceManager _rm =
        new("CloudRedirect.Resources.Strings", typeof(S).Assembly);

    /// <summary>
    /// Returns the localized string for the given key, or the key itself if not found.
    /// </summary>
    public static string Get(string key)
    {
        return _rm.GetString(key, CultureInfo.CurrentUICulture) ?? key;
    }

    /// <summary>
    /// Returns the localized string for the given key, formatted with the provided arguments.
    /// Falls back to the key itself if not found.
    /// </summary>
    public static string Format(string key, params object[] args)
    {
        var template = _rm.GetString(key, CultureInfo.CurrentUICulture);
        if (template == null) return key;
        return string.Format(CultureInfo.CurrentCulture, template, args);
    }
}
