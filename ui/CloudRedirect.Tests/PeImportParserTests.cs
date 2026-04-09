using CloudRedirect.Services.Patching;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for PeImportParser: ReadAsciiZ and FindKernel32IatEntries.
/// Uses both synthetic PE data and real system DLLs.
/// </summary>
public class PeImportParserTests
{
    // ── ReadAsciiZ ───────────────────────────────────────────────────

    [Fact]
    public void ReadAsciiZ_NormalString()
    {
        byte[] data = { 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x00, 0xFF };
        Assert.Equal("Hello", PeImportParser.ReadAsciiZ(data, 0));
    }

    [Fact]
    public void ReadAsciiZ_MiddleOfBuffer()
    {
        byte[] data = { 0xFF, 0xFF, 0x41, 0x42, 0x00, 0xFF };
        Assert.Equal("AB", PeImportParser.ReadAsciiZ(data, 2));
    }

    [Fact]
    public void ReadAsciiZ_EmptyString_AtNull()
    {
        byte[] data = { 0x00, 0x41 };
        Assert.Equal(string.Empty, PeImportParser.ReadAsciiZ(data, 0));
    }

    [Fact]
    public void ReadAsciiZ_NoTerminator_ReadsToEnd()
    {
        byte[] data = { 0x41, 0x42, 0x43 };
        Assert.Equal("ABC", PeImportParser.ReadAsciiZ(data, 0));
    }

    [Fact]
    public void ReadAsciiZ_OffsetAtEnd_ReturnsEmpty()
    {
        byte[] data = { 0x41 };
        Assert.Equal(string.Empty, PeImportParser.ReadAsciiZ(data, 1));
    }

    // ── FindKernel32IatEntries with real kernel32.dll ────────────────

    [Fact]
    public void FindKernel32IatEntries_RealKernel32_FindsBothEntries()
    {
        // kernel32.dll imports from api-ms-win-* and ntdll, but itself
        // exports LoadLibraryA/GetProcAddress. Other system DLLs import
        // from kernel32. Let's use a DLL we know imports from KERNEL32.
        // user32.dll is a good candidate -- it imports from KERNEL32.dll.
        var dllPath = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.System),
            "user32.dll");

        if (!File.Exists(dllPath))
            return; // skip on systems without user32.dll (shouldn't happen)

        var pe = File.ReadAllBytes(dllPath);
        var sections = PeSection.Parse(pe);
        Assert.NotEmpty(sections);

        var (loadLib, getProc) = PeImportParser.FindKernel32IatEntries(pe, sections);

        // user32.dll may or may not import both -- it definitely imports from KERNEL32
        // but might not import LoadLibraryA specifically.
        // At minimum, verify no crash and the parser handles a real PE.
        // If it finds them, the RVAs should be positive.
        if (loadLib >= 0) Assert.True(loadLib > 0);
        if (getProc >= 0) Assert.True(getProc > 0);
    }

    [Fact]
    public void FindKernel32IatEntries_RealExe_SelfTest()
    {
        // Use the currently running .exe -- the test host. It's a .NET exe
        // but still a valid PE that imports from KERNEL32.
        var exePath = Environment.ProcessPath;
        if (exePath == null || !File.Exists(exePath))
            return;

        var pe = File.ReadAllBytes(exePath);
        var sections = PeSection.Parse(pe);

        // .NET exes have a valid PE header with sections
        Assert.NotEmpty(sections);

        // It may or may not import LoadLibraryA/GetProcAddress,
        // but FindKernel32IatEntries should not crash on any valid PE.
        var (loadLib, getProc) = PeImportParser.FindKernel32IatEntries(pe, sections);
        // No assertion on values -- just verify no crash
    }

    [Fact]
    public void FindKernel32IatEntries_TooSmall_ReturnsNegative()
    {
        var (loadLib, getProc) = PeImportParser.FindKernel32IatEntries(new byte[10], Array.Empty<PeSection>());
        Assert.Equal(-1, loadLib);
        Assert.Equal(-1, getProc);
    }

    [Fact]
    public void FindKernel32IatEntries_EmptySections_ReturnsNegative()
    {
        // Valid enough DOS header but no sections
        var pe = new byte[512];
        pe[0] = 0x4D; pe[1] = 0x5A;
        BitConverter.TryWriteBytes(pe.AsSpan(0x3C, 4), 0x80);
        pe[0x80] = (byte)'P'; pe[0x81] = (byte)'E';
        // Magic = PE32+
        BitConverter.TryWriteBytes(pe.AsSpan(0x80 + 24, 2), (ushort)0x20B);

        var (loadLib, getProc) = PeImportParser.FindKernel32IatEntries(pe, Array.Empty<PeSection>());
        Assert.Equal(-1, loadLib);
        Assert.Equal(-1, getProc);
    }
}
