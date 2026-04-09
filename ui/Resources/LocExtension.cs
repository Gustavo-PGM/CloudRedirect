using System;
using System.Windows.Markup;

namespace CloudRedirect.Resources;

/// <summary>
/// XAML markup extension for localized strings.
/// Usage: <c>{res:Loc MainWindow_Title}</c>
/// </summary>
[MarkupExtensionReturnType(typeof(string))]
public class LocExtension : MarkupExtension
{
    public string Key { get; set; }

    public LocExtension() => Key = "";
    public LocExtension(string key) => Key = key;

    public override object ProvideValue(IServiceProvider serviceProvider)
    {
        if (string.IsNullOrEmpty(Key))
            return "";
        return S.Get(Key);
    }
}
