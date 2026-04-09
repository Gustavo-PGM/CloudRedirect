using CloudRedirect.Services;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for CloudCleanup.IsSelfUnlockingLua -- the heuristic that distinguishes
/// self-unlocking luas (base game not owned) from DLC-only luas (base game owned).
/// </summary>
public class LuaDetectionTests
{
    private string WriteTempLua(string content)
    {
        var path = Path.Combine(Path.GetTempPath(),
            $"CloudRedirect_Tests_{Guid.NewGuid():N}.lua");
        File.WriteAllText(path, content);
        return path;
    }

    // ── Self-unlocking luas (should return true) ─────────────────────

    [Fact]
    public void SelfUnlocking_SimpleCall()
    {
        var path = WriteTempLua("addappid(12345)\n");
        try { Assert.True(CloudCleanup.IsSelfUnlockingLua(path, 12345)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void SelfUnlocking_WithDepotArgs()
    {
        // Morrenus format: addappid(12345, 1, "hash") -- comment
        var path = WriteTempLua(
            "addappid(12345, 1, \"abc123\") -- Main Depot\n");
        try { Assert.True(CloudCleanup.IsSelfUnlockingLua(path, 12345)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void SelfUnlocking_WithLeadingWhitespace()
    {
        var path = WriteTempLua("  \taddappid(12345)\n");
        try { Assert.True(CloudCleanup.IsSelfUnlockingLua(path, 12345)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void SelfUnlocking_FullMorrenusFormat()
    {
        // Realistic Morrenus-generated self-unlocking lua
        var content = """
            -- 1032760's Lua and Manifest Created by Morrenus
            -- Phoenix Wright: Ace Attorney Trilogy - Turnabout Tunes
            -- Created: September 30, 2025 at 06:32:12 EDT
            -- Website: https://manifest.morrenus.xyz/
            -- Total Depots: 6
            -- Total DLCs: 0

            -- MAIN APPLICATION
            addappid(1032760) -- Phoenix Wright: Ace Attorney Trilogy - Turnabout Tunes
            -- MAIN APP DEPOTS
            addappid(1032761, 1, "31e5103081f6718fb15d5632a83a541b24e13599f8c4e18ccb5235300d4f6cf7")
            addappid(1032762, 1, "7ba072ffb7c2d7d67aedfb29bddbedf50f9e0bc270ad1b7a63a731a0e7a6f90e")
            """;
        var path = WriteTempLua(content);
        try { Assert.True(CloudCleanup.IsSelfUnlockingLua(path, 1032760)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void SelfUnlocking_AmongOtherCalls()
    {
        // Self-unlock call mixed with DLC calls
        var content = "addappid(99999)\naddappid(12345)\naddappid(88888)\n";
        var path = WriteTempLua(content);
        try { Assert.True(CloudCleanup.IsSelfUnlockingLua(path, 12345)); }
        finally { File.Delete(path); }
    }

    // ── DLC-only luas (should return false) ──────────────────────────

    [Fact]
    public void DlcOnly_NoSelfUnlock()
    {
        // DLC-only: calls addappid for different IDs, never for 3500390 itself
        var content = """
            -- 3500390's DLC Lua Created by Morrenus
            -- Mega Man Star Force Legacy Collection
            addappid(4059840)
            addappid(4059840, 1, "e36991eb8125945b9f99b2c11323010a5a918ad3f07b8bb7dc199a5cb4c49b42")
            addappid(4059860)
            addappid(4059860, 1, "a4a1b191f1d28dd691f0e9197d0f156a0781b407a466d44a7e455aa97903b032")
            """;
        var path = WriteTempLua(content);
        try { Assert.False(CloudCleanup.IsSelfUnlockingLua(path, 3500390)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void DlcOnly_EmptyFile()
    {
        var path = WriteTempLua("");
        try { Assert.False(CloudCleanup.IsSelfUnlockingLua(path, 12345)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void DlcOnly_OnlyComments()
    {
        var content = "-- some comment\n-- addappid(12345)\n-- another comment\n";
        var path = WriteTempLua(content);
        try { Assert.False(CloudCleanup.IsSelfUnlockingLua(path, 12345)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void DlcOnly_CommentedOutSelfUnlock()
    {
        // The self-unlock line is commented out -- should NOT count
        var content = "-- addappid(12345)\naddappid(67890)\n";
        var path = WriteTempLua(content);
        try { Assert.False(CloudCleanup.IsSelfUnlockingLua(path, 12345)); }
        finally { File.Delete(path); }
    }

    // ── Edge cases ───────────────────────────────────────────────────

    [Fact]
    public void NonexistentFile_ReturnsFalse()
    {
        Assert.False(CloudCleanup.IsSelfUnlockingLua(
            @"C:\nonexistent_path\fake.lua", 12345));
    }

    [Fact]
    public void PartialAppIdMatch_DoesNotFalsePositive()
    {
        // appId 123 should NOT match addappid(12345) -- the ")" or "," delimiter prevents this
        var path = WriteTempLua("addappid(12345)\n");
        try { Assert.False(CloudCleanup.IsSelfUnlockingLua(path, 123)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void PartialAppIdMatch_Suffix_DoesNotFalsePositive()
    {
        // appId 2345 should NOT match addappid(12345)
        var path = WriteTempLua("addappid(12345)\n");
        try { Assert.False(CloudCleanup.IsSelfUnlockingLua(path, 2345)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void SelfUnlocking_WindowsLineEndings()
    {
        var path = WriteTempLua("-- comment\r\naddappid(12345)\r\naddappid(67890)\r\n");
        try { Assert.True(CloudCleanup.IsSelfUnlockingLua(path, 12345)); }
        finally { File.Delete(path); }
    }

    [Fact]
    public void SelfUnlocking_NoTrailingNewline()
    {
        var path = WriteTempLua("addappid(12345)");
        try { Assert.True(CloudCleanup.IsSelfUnlockingLua(path, 12345)); }
        finally { File.Delete(path); }
    }
}
