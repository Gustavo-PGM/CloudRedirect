using CloudRedirect.Services.Patching;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for PeSection: RVA/file-offset conversion, section parsing,
/// and lookup methods. Uses hand-crafted minimal PE headers.
/// </summary>
public class PeSectionTests
{
    // Helper: build a minimal PE32+ (x64) binary with the given sections.
    // Returns a byte array that PeSection.Parse can handle.
    static byte[] BuildMinimalPe64(params (string name, int va, int vsize, int rawOff, int rawSize, uint chars)[] sections)
    {
        // We need: DOS header (64 bytes min), PE signature, COFF header, optional header, section headers.
        // Place PE header at offset 0x40 (the minimum valid e_lfanew).
        int peOff = 0x40;
        int coffSize = 24;       // PE sig (4) + COFF header (20)
        int optSize = 240;       // PE32+ optional header size (standard 24 + windows-specific 88 + data dirs 16*8=128)
        int sectionStart = peOff + coffSize + optSize;
        int totalSize = sectionStart + sections.Length * 40;

        // Make sure we have enough bytes for the largest raw offset + raw size
        foreach (var s in sections)
        {
            int needed = s.rawOff + s.rawSize;
            if (needed > totalSize) totalSize = needed;
        }

        var pe = new byte[totalSize];

        // DOS header: e_magic = "MZ", e_lfanew at 0x3C
        pe[0] = 0x4D; pe[1] = 0x5A;
        BitConverter.TryWriteBytes(pe.AsSpan(0x3C, 4), peOff);

        // PE signature
        pe[peOff] = (byte)'P'; pe[peOff + 1] = (byte)'E';

        // COFF header
        BitConverter.TryWriteBytes(pe.AsSpan(peOff + 6, 2), (ushort)sections.Length);
        BitConverter.TryWriteBytes(pe.AsSpan(peOff + 20, 2), (ushort)optSize);

        // Optional header: magic = PE32+ (0x20B)
        BitConverter.TryWriteBytes(pe.AsSpan(peOff + 24, 2), (ushort)0x20B);

        // Image base at peOff+24+24 (PE32+)
        BitConverter.TryWriteBytes(pe.AsSpan(peOff + 24 + 24, 8), (long)0x180000000);

        // Section headers
        for (int i = 0; i < sections.Length; i++)
        {
            int off = sectionStart + i * 40;
            var (name, va, vsize, rawOffset, rawSize, chars) = sections[i];

            // Name (8 bytes, null-padded)
            for (int j = 0; j < Math.Min(name.Length, 8); j++)
                pe[off + j] = (byte)name[j];

            BitConverter.TryWriteBytes(pe.AsSpan(off + 8, 4), vsize);
            BitConverter.TryWriteBytes(pe.AsSpan(off + 12, 4), va);
            BitConverter.TryWriteBytes(pe.AsSpan(off + 16, 4), rawSize);
            BitConverter.TryWriteBytes(pe.AsSpan(off + 20, 4), rawOffset);
            BitConverter.TryWriteBytes(pe.AsSpan(off + 36, 4), chars);
        }

        return pe;
    }

    static readonly PeSection TextSection = new(".text", 0x1000, 0x5000, 0x400, 0x5000, 0x60000020);
    static readonly PeSection RdataSection = new(".rdata", 0x6000, 0x2000, 0x5400, 0x2000, 0x40000040);
    static readonly PeSection DataSection = new(".data", 0x8000, 0x3000, 0x7400, 0x1000, 0xC0000040);
    // .data has VirtualSize > RawSize => BSS region from 0x9000..0xB000

    static readonly PeSection[] TestSections = { TextSection, RdataSection, DataSection };

    // ── RvaToFileOffset ──────────────────────────────────────────────

    [Fact]
    public void RvaToFileOffset_TextSectionStart()
    {
        Assert.Equal(0x400, PeSection.RvaToFileOffset(TestSections, 0x1000));
    }

    [Fact]
    public void RvaToFileOffset_TextSectionMiddle()
    {
        Assert.Equal(0x400 + 0x100, PeSection.RvaToFileOffset(TestSections, 0x1100));
    }

    [Fact]
    public void RvaToFileOffset_TextSectionEnd()
    {
        // Last byte of .text raw data
        Assert.Equal(0x400 + 0x4FFF, PeSection.RvaToFileOffset(TestSections, 0x1000 + 0x4FFF));
    }

    [Fact]
    public void RvaToFileOffset_RdataSection()
    {
        Assert.Equal(0x5400, PeSection.RvaToFileOffset(TestSections, 0x6000));
    }

    [Fact]
    public void RvaToFileOffset_BssRegion_ReturnsNegative()
    {
        // .data VA=0x8000, VSize=0x3000, RawSize=0x1000. RVA 0x9000 is in BSS (no file backing).
        Assert.Equal(-1, PeSection.RvaToFileOffset(TestSections, 0x9000));
    }

    [Fact]
    public void RvaToFileOffset_BeyondAllSections_ReturnsNegative()
    {
        Assert.Equal(-1, PeSection.RvaToFileOffset(TestSections, 0xF0000));
    }

    [Fact]
    public void RvaToFileOffset_Zero_ReturnsNegative()
    {
        Assert.Equal(-1, PeSection.RvaToFileOffset(TestSections, 0));
    }

    // ── FileOffsetToRva ──────────────────────────────────────────────

    [Fact]
    public void FileOffsetToRva_TextSectionStart()
    {
        Assert.Equal(0x1000, PeSection.FileOffsetToRva(TestSections, 0x400));
    }

    [Fact]
    public void FileOffsetToRva_RdataMiddle()
    {
        Assert.Equal(0x6100, PeSection.FileOffsetToRva(TestSections, 0x5500));
    }

    [Fact]
    public void FileOffsetToRva_OutsideAllSections()
    {
        Assert.Equal(-1, PeSection.FileOffsetToRva(TestSections, 0x100));
    }

    // ── Roundtrip ────────────────────────────────────────────────────

    [Theory]
    [InlineData(0x1000)]
    [InlineData(0x1234)]
    [InlineData(0x6800)]
    [InlineData(0x8500)]  // inside .data raw region
    public void RvaToFileOffset_FileOffsetToRva_Roundtrip(int rva)
    {
        int fileOff = PeSection.RvaToFileOffset(TestSections, rva);
        Assert.NotEqual(-1, fileOff);
        Assert.Equal(rva, PeSection.FileOffsetToRva(TestSections, fileOff));
    }

    // ── FindByRva ────────────────────────────────────────────────────

    [Fact]
    public void FindByRva_TextSection()
    {
        var sec = PeSection.FindByRva(TestSections, 0x1500);
        Assert.NotNull(sec);
        Assert.Equal(".text", sec.Value.Name);
    }

    [Fact]
    public void FindByRva_BssRegion_StillFindsSection()
    {
        // BSS is in virtual range but not file-backed. FindByRva should still find it.
        var sec = PeSection.FindByRva(TestSections, 0x9500);
        Assert.NotNull(sec);
        Assert.Equal(".data", sec.Value.Name);
    }

    [Fact]
    public void FindByRva_OutsideAll_ReturnsNull()
    {
        Assert.Null(PeSection.FindByRva(TestSections, 0xF0000));
    }

    // ── FindByFileOffset ─────────────────────────────────────────────

    [Fact]
    public void FindByFileOffset_TextSection()
    {
        var sec = PeSection.FindByFileOffset(TestSections, 0x500);
        Assert.NotNull(sec);
        Assert.Equal(".text", sec.Value.Name);
    }

    [Fact]
    public void FindByFileOffset_OutsideAll()
    {
        Assert.Null(PeSection.FindByFileOffset(TestSections, 0x100));
    }

    // ── Find (by name) ───────────────────────────────────────────────

    [Fact]
    public void Find_ExistingSection()
    {
        var sec = PeSection.Find(TestSections, ".rdata");
        Assert.NotNull(sec);
        Assert.Equal(0x6000u, sec.Value.VirtualAddress);
    }

    [Fact]
    public void Find_NonExistentSection()
    {
        Assert.Null(PeSection.Find(TestSections, ".rsrc"));
    }

    // ── IsExecutable ─────────────────────────────────────────────────

    [Fact]
    public void IsExecutable_TextSection_True()
    {
        Assert.True(TextSection.IsExecutable);
    }

    [Fact]
    public void IsExecutable_DataSection_False()
    {
        Assert.False(DataSection.IsExecutable);
    }

    // ── Parse ─────────────────────────────────────────────────────────

    [Fact]
    public void Parse_MinimalPe64_ReturnsSections()
    {
        var pe = BuildMinimalPe64(
            (".text", 0x1000, 0x5000, 0x400, 0x5000, 0x60000020),
            (".rdata", 0x6000, 0x2000, 0x5400, 0x2000, 0x40000040)
        );

        var sections = PeSection.Parse(pe);

        Assert.Equal(2, sections.Length);
        Assert.Equal(".text", sections[0].Name);
        Assert.Equal(0x1000u, sections[0].VirtualAddress);
        Assert.Equal(0x5000u, sections[0].VirtualSize);
        Assert.Equal(0x400u, sections[0].RawOffset);
        Assert.Equal(0x5000u, sections[0].RawSize);
        Assert.True(sections[0].IsExecutable);

        Assert.Equal(".rdata", sections[1].Name);
        Assert.False(sections[1].IsExecutable);
    }

    [Fact]
    public void Parse_TooSmall_ReturnsEmpty()
    {
        Assert.Empty(PeSection.Parse(new byte[10]));
    }

    [Fact]
    public void Parse_BadMagic_ReturnsEmpty()
    {
        var pe = new byte[256];
        // Valid e_lfanew but no PE signature
        BitConverter.TryWriteBytes(pe.AsSpan(0x3C, 4), 0x40);
        pe[0x40] = (byte)'X';

        Assert.Empty(PeSection.Parse(pe));
    }

    // ── Empty sections array ─────────────────────────────────────────

    [Fact]
    public void EmptySections_AllMethodsReturnSafely()
    {
        var empty = Array.Empty<PeSection>();
        Assert.Equal(-1, PeSection.RvaToFileOffset(empty, 0x1000));
        Assert.Equal(-1, PeSection.FileOffsetToRva(empty, 0x400));
        Assert.Null(PeSection.FindByRva(empty, 0x1000));
        Assert.Null(PeSection.FindByFileOffset(empty, 0x400));
        Assert.Null(PeSection.Find(empty, ".text"));
    }
}
