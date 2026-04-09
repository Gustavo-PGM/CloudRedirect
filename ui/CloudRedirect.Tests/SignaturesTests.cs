using CloudRedirect.Services.Patching;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for Signatures: pattern scanning with masks, exact byte scanning,
/// and the specific FindCorePatchN / FindPayloadPatchN finders.
/// </summary>
public class SignaturesTests
{
    // ── ScanForBytes ─────────────────────────────────────────────────

    [Fact]
    public void ScanForBytes_FindsNeedleAtStart()
    {
        byte[] data = { 0xAA, 0xBB, 0xCC, 0xDD };
        byte[] needle = { 0xAA, 0xBB };
        Assert.Equal(0, Signatures.ScanForBytes(data, 0, data.Length, needle));
    }

    [Fact]
    public void ScanForBytes_FindsNeedleInMiddle()
    {
        byte[] data = { 0x00, 0x00, 0xAA, 0xBB, 0xCC, 0x00 };
        byte[] needle = { 0xAA, 0xBB, 0xCC };
        Assert.Equal(2, Signatures.ScanForBytes(data, 0, data.Length, needle));
    }

    [Fact]
    public void ScanForBytes_FindsNeedleAtEnd()
    {
        byte[] data = { 0x00, 0x00, 0xAA, 0xBB };
        byte[] needle = { 0xAA, 0xBB };
        Assert.Equal(2, Signatures.ScanForBytes(data, 0, data.Length, needle));
    }

    [Fact]
    public void ScanForBytes_NotFound_ReturnsNegative()
    {
        byte[] data = { 0x00, 0x01, 0x02, 0x03 };
        byte[] needle = { 0xFF, 0xFE };
        Assert.Equal(-1, Signatures.ScanForBytes(data, 0, data.Length, needle));
    }

    [Fact]
    public void ScanForBytes_RespectsStartOffset()
    {
        byte[] data = { 0xAA, 0xBB, 0x00, 0xAA, 0xBB };
        byte[] needle = { 0xAA, 0xBB };
        Assert.Equal(3, Signatures.ScanForBytes(data, 1, data.Length, needle));
    }

    [Fact]
    public void ScanForBytes_RespectsEndBound()
    {
        byte[] data = { 0x00, 0x00, 0xAA, 0xBB, 0xCC };
        byte[] needle = { 0xAA, 0xBB, 0xCC };
        // end=4 means the needle starting at index 2 would need indices 2,3,4 -- but 4 < end wouldn't hold
        Assert.Equal(-1, Signatures.ScanForBytes(data, 0, 4, needle));
    }

    [Fact]
    public void ScanForBytes_EmptyNeedle_ReturnsStart()
    {
        byte[] data = { 0x00, 0x01 };
        byte[] needle = Array.Empty<byte>();
        Assert.Equal(0, Signatures.ScanForBytes(data, 0, data.Length, needle));
    }

    // ── ScanForPattern (with mask) ───────────────────────────────────

    [Fact]
    public void ScanForPattern_ExactMatch()
    {
        byte[] data = { 0x00, 0xE8, 0x10, 0x20, 0x85 };
        byte[] pattern = { 0xE8, 0x10, 0x20, 0x85 };
        byte[] mask =    { 0xFF, 0xFF, 0xFF, 0xFF };
        Assert.Equal(1, Signatures.ScanForPattern(data, 0, data.Length, pattern, mask));
    }

    [Fact]
    public void ScanForPattern_WildcardBytes()
    {
        byte[] data = { 0xE8, 0xAA, 0xBB, 0xCC, 0xDD, 0x85, 0xC0 };
        byte[] pattern = { 0xE8, 0x00, 0x00, 0x00, 0x00, 0x85, 0xC0 };
        byte[] mask =    { 0xFF, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF };
        Assert.Equal(0, Signatures.ScanForPattern(data, 0, data.Length, pattern, mask));
    }

    [Fact]
    public void ScanForPattern_NoMatch()
    {
        byte[] data = { 0xE8, 0xAA, 0xBB, 0xCC, 0xDD, 0x84, 0xC0 };
        byte[] pattern = { 0xE8, 0x00, 0x00, 0x00, 0x00, 0x85, 0xC0 };
        byte[] mask =    { 0xFF, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF };
        Assert.Equal(-1, Signatures.ScanForPattern(data, 0, data.Length, pattern, mask));
    }

    [Fact]
    public void ScanForPattern_AllWildcard_MatchesAtStart()
    {
        byte[] data = { 0x12, 0x34, 0x56 };
        byte[] pattern = { 0x00, 0x00 };
        byte[] mask =    { 0x00, 0x00 };
        Assert.Equal(0, Signatures.ScanForPattern(data, 0, data.Length, pattern, mask));
    }

    // ── FindCorePatch1 ───────────────────────────────────────────────

    [Fact]
    public void FindCorePatch1_MatchesNegativeCallTarget()
    {
        // E8 [negative rel32] 85 C0 0F 84
        byte[] data = new byte[64];
        int pos = 10;
        data[pos] = 0xE8;
        // negative relative: -0x100 = 0xFFFFFF00
        BitConverter.TryWriteBytes(data.AsSpan(pos + 1, 4), -0x100);
        data[pos + 5] = 0x85;
        data[pos + 6] = 0xC0;
        data[pos + 7] = 0x0F;
        data[pos + 8] = 0x84;

        Assert.Equal(pos, Signatures.FindCorePatch1(data, 0, data.Length));
    }

    [Fact]
    public void FindCorePatch1_IgnoresPositiveCallTarget()
    {
        byte[] data = new byte[64];
        int pos = 10;
        data[pos] = 0xE8;
        BitConverter.TryWriteBytes(data.AsSpan(pos + 1, 4), 0x100); // positive
        data[pos + 5] = 0x85;
        data[pos + 6] = 0xC0;
        data[pos + 7] = 0x0F;
        data[pos + 8] = 0x84;

        Assert.Equal(-1, Signatures.FindCorePatch1(data, 0, data.Length));
    }

    // ── FindCorePatch2 ───────────────────────────────────────────────

    [Fact]
    public void FindCorePatch2_Strict_85C0_74_33FF()
    {
        byte[] data = new byte[32];
        int pos = 5;
        data[pos] = 0x85; data[pos + 1] = 0xC0;
        data[pos + 2] = 0x74; data[pos + 3] = 0x10; // jz short
        data[pos + 4] = 0x33; data[pos + 5] = 0xFF; // xor edi, edi

        // FindCorePatch2 returns pos+2 (the jz instruction)
        Assert.Equal(pos + 2, Signatures.FindCorePatch2(data, 0, data.Length));
    }

    [Fact]
    public void FindCorePatch2_LooserMatch_74_33FF_E9()
    {
        byte[] data = new byte[32];
        int pos = 5;
        data[pos] = 0x74; data[pos + 1] = 0x10;
        data[pos + 2] = 0x33; data[pos + 3] = 0xFF;
        data[pos + 4] = 0xE9;

        Assert.Equal(pos, Signatures.FindCorePatch2(data, 0, data.Length));
    }

    // ── FindPayloadPatch3 (Spacewar anchor) ──────────────────────────

    [Fact]
    public void FindPayloadPatch3_FindsAfterSpacewarAnchor()
    {
        // Spacewar constant: C7 40 09 E0 01 00 00
        // Followed within 30 bytes by: 89 3D
        byte[] data = new byte[64];
        int anchor = 10;
        byte[] spacewar = { 0xC7, 0x40, 0x09, 0xE0, 0x01, 0x00, 0x00 };
        spacewar.CopyTo(data, anchor);

        int targetPos = anchor + spacewar.Length + 5;
        data[targetPos] = 0x89;
        data[targetPos + 1] = 0x3D;

        Assert.Equal(targetPos, Signatures.FindPayloadPatch3(data, 0, data.Length));
    }

    [Fact]
    public void FindPayloadPatch3_NoSpacewarAnchor_ReturnsNegative()
    {
        byte[] data = new byte[64];
        Assert.Equal(-1, Signatures.FindPayloadPatch3(data, 0, data.Length));
    }

    // ── Edge cases ───────────────────────────────────────────────────

    [Fact]
    public void ScanForBytes_DataSmallerThanNeedle()
    {
        byte[] data = { 0xAA };
        byte[] needle = { 0xAA, 0xBB };
        Assert.Equal(-1, Signatures.ScanForBytes(data, 0, data.Length, needle));
    }

    [Fact]
    public void ScanForPattern_EmptyData()
    {
        byte[] data = Array.Empty<byte>();
        byte[] pattern = { 0xE8 };
        byte[] mask = { 0xFF };
        Assert.Equal(-1, Signatures.ScanForPattern(data, 0, 0, pattern, mask));
    }
}
