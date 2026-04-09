using CloudRedirect.Services.Patching;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Integration tests that read real PE files (steamclient64.dll, user32.dll)
/// to verify PeSection.Parse produces structurally valid results against
/// real-world binaries, not just synthetic test data.
/// Tests are skipped if the required files are not present.
/// </summary>
public class PeIntegrationTests
{
    private const string SteamClient64Path = @"C:\Games\Steam\steamclient64.dll";

    [Fact]
    public void Parse_SteamClient64_ReturnsMultipleSections()
    {
        if (!File.Exists(SteamClient64Path))
            return; // skip on machines without Steam

        var pe = File.ReadAllBytes(SteamClient64Path);
        var sections = PeSection.Parse(pe);

        // steamclient64.dll is a large DLL; it should have several sections
        Assert.True(sections.Length >= 3, $"Expected >= 3 sections, got {sections.Length}");

        // Verify all section names are non-empty
        foreach (var s in sections)
        {
            Assert.False(string.IsNullOrEmpty(s.Name), "Section name should not be empty");
        }
    }

    [Fact]
    public void Parse_SteamClient64_HasTextSection()
    {
        if (!File.Exists(SteamClient64Path))
            return;

        var pe = File.ReadAllBytes(SteamClient64Path);
        var sections = PeSection.Parse(pe);

        var text = PeSection.Find(sections, ".text");
        Assert.NotNull(text);
        Assert.True(text.Value.IsExecutable, ".text should be executable");
        Assert.True(text.Value.VirtualSize > 0, ".text VirtualSize should be > 0");
        Assert.True(text.Value.RawSize > 0, ".text RawSize should be > 0");
    }

    [Fact]
    public void Parse_SteamClient64_HasRdataSection()
    {
        if (!File.Exists(SteamClient64Path))
            return;

        var pe = File.ReadAllBytes(SteamClient64Path);
        var sections = PeSection.Parse(pe);

        var rdata = PeSection.Find(sections, ".rdata");
        Assert.NotNull(rdata);
        Assert.False(rdata.Value.IsExecutable, ".rdata should not be executable");
    }

    [Fact]
    public void Parse_SteamClient64_SectionsHaveValidLayout()
    {
        if (!File.Exists(SteamClient64Path))
            return;

        var pe = File.ReadAllBytes(SteamClient64Path);
        var sections = PeSection.Parse(pe);

        for (int i = 0; i < sections.Length; i++)
        {
            var s = sections[i];

            // Virtual address should be positive (sections don't start at 0)
            Assert.True(s.VirtualAddress > 0, $"{s.Name}: VA should be > 0");

            // Raw offset should be within the file
            Assert.True(s.RawOffset < (uint)pe.Length, $"{s.Name}: RawOffset should be within file");

            // Raw data should not extend beyond the file
            Assert.True(s.RawOffset + s.RawSize <= (uint)pe.Length,
                $"{s.Name}: raw data extends beyond file ({s.RawOffset} + {s.RawSize} > {pe.Length})");
        }

        // Sections should be ordered by VirtualAddress (standard PE convention)
        for (int i = 1; i < sections.Length; i++)
        {
            Assert.True(sections[i].VirtualAddress > sections[i - 1].VirtualAddress,
                $"Sections should be ordered by VA: {sections[i - 1].Name} (0x{sections[i - 1].VirtualAddress:X}) >= {sections[i].Name} (0x{sections[i].VirtualAddress:X})");
        }
    }

    [Fact]
    public void Parse_SteamClient64_RvaRoundtrip()
    {
        if (!File.Exists(SteamClient64Path))
            return;

        var pe = File.ReadAllBytes(SteamClient64Path);
        var sections = PeSection.Parse(pe);

        // Pick a few RVAs in the .text section and verify roundtrip
        var text = PeSection.Find(sections, ".text");
        Assert.NotNull(text);

        int[] testRvas = {
            (int)text.Value.VirtualAddress,
            (int)text.Value.VirtualAddress + 0x100,
            (int)(text.Value.VirtualAddress + text.Value.RawSize - 1),
        };

        foreach (int rva in testRvas)
        {
            int fileOff = PeSection.RvaToFileOffset(sections, rva);
            Assert.NotEqual(-1, fileOff);
            int roundtrip = PeSection.FileOffsetToRva(sections, fileOff);
            Assert.Equal(rva, roundtrip);
        }
    }

    // ── user32.dll sanity check (should always exist on Windows) ─────

    [Fact]
    public void Parse_User32Dll_HasTextSection()
    {
        var path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), "user32.dll");
        if (!File.Exists(path))
            return;

        var pe = File.ReadAllBytes(path);
        var sections = PeSection.Parse(pe);

        Assert.True(sections.Length >= 2, $"user32.dll should have >= 2 sections, got {sections.Length}");

        var text = PeSection.Find(sections, ".text");
        Assert.NotNull(text);
        Assert.True(text.Value.IsExecutable);
    }
}
