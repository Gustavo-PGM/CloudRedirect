using CloudRedirect.Services;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for SteamStoreClient: URL validation and singleton behavior.
/// Network-dependent API tests are not included (would require mocking HttpClient).
/// </summary>
public class SteamStoreClientTests
{
    // ── IsValidSteamCdnUrl ─────────────────────────────────────────────

    [Fact]
    public void IsValidSteamCdnUrl_NullReturnsFalse()
    {
        Assert.False(SteamStoreClient.IsValidSteamCdnUrl(null));
    }

    [Fact]
    public void IsValidSteamCdnUrl_EmptyReturnsFalse()
    {
        Assert.False(SteamStoreClient.IsValidSteamCdnUrl(""));
    }

    [Fact]
    public void IsValidSteamCdnUrl_ValidSteamstaticUrl()
    {
        Assert.True(SteamStoreClient.IsValidSteamCdnUrl(
            "https://shared.steamstatic.com/store_item_assets/steam/apps/1221480/header.jpg"));
    }

    [Fact]
    public void IsValidSteamCdnUrl_ValidSteampoweredUrl()
    {
        Assert.True(SteamStoreClient.IsValidSteamCdnUrl(
            "https://cdn.steampowered.com/apps/1221480/header.jpg"));
    }

    [Fact]
    public void IsValidSteamCdnUrl_ValidAkamaiUrl()
    {
        Assert.True(SteamStoreClient.IsValidSteamCdnUrl(
            "https://cdn.steamcdn-a.akamaihd.net/steam/apps/1221480/header.jpg"));
    }

    [Fact]
    public void IsValidSteamCdnUrl_HttpRejected()
    {
        Assert.False(SteamStoreClient.IsValidSteamCdnUrl(
            "http://shared.steamstatic.com/store_item_assets/steam/apps/1221480/header.jpg"));
    }

    [Fact]
    public void IsValidSteamCdnUrl_NonSteamDomainRejected()
    {
        Assert.False(SteamStoreClient.IsValidSteamCdnUrl(
            "https://evil.example.com/header.jpg"));
    }

    [Fact]
    public void IsValidSteamCdnUrl_MalformedUrlRejected()
    {
        Assert.False(SteamStoreClient.IsValidSteamCdnUrl("not-a-url"));
    }

    [Fact]
    public void IsValidSteamCdnUrl_FileSchemeRejected()
    {
        Assert.False(SteamStoreClient.IsValidSteamCdnUrl("file:///C:/Windows/System32/calc.exe"));
    }

    [Fact]
    public void IsValidSteamCdnUrl_SpoofedSubdomainRejected()
    {
        // "steamstatic.com.evil.com" should not match ".steamstatic.com"
        Assert.False(SteamStoreClient.IsValidSteamCdnUrl(
            "https://steamstatic.com.evil.com/header.jpg"));
    }

    [Fact]
    public void IsValidSteamCdnUrl_JavascriptSchemeRejected()
    {
        Assert.False(SteamStoreClient.IsValidSteamCdnUrl("javascript:alert(1)"));
    }

    [Fact]
    public void IsValidSteamCdnUrl_DataSchemeRejected()
    {
        Assert.False(SteamStoreClient.IsValidSteamCdnUrl("data:text/html,<script>alert(1)</script>"));
    }

    // ── Singleton behavior ─────────────────────────────────────────────

    [Fact]
    public void SharedInstance_ReturnsSameInstance()
    {
        var a = SteamStoreClient.Shared;
        var b = SteamStoreClient.Shared;
        Assert.Same(a, b);
    }
}
