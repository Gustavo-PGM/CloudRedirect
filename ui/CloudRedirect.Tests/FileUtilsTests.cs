using CloudRedirect.Services;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for FileUtils: atomic write operations and FormatSize.
/// </summary>
public class FileUtilsTests
{
    private string GetTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "CloudRedirect_Tests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    // ── AtomicWriteAllBytes ──────────────────────────────────────────

    [Fact]
    public void AtomicWriteAllBytes_CreatesFile()
    {
        var dir = GetTempDir();
        try
        {
            var path = Path.Combine(dir, "test.bin");
            var data = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };

            FileUtils.AtomicWriteAllBytes(path, data);

            Assert.True(File.Exists(path));
            Assert.Equal(data, File.ReadAllBytes(path));
            Assert.False(File.Exists(path + ".tmp")); // temp file cleaned up
        }
        finally { Directory.Delete(dir, true); }
    }

    [Fact]
    public void AtomicWriteAllBytes_OverwritesExistingFile()
    {
        var dir = GetTempDir();
        try
        {
            var path = Path.Combine(dir, "test.bin");
            File.WriteAllBytes(path, new byte[] { 0x01 });

            var newData = new byte[] { 0x02, 0x03 };
            FileUtils.AtomicWriteAllBytes(path, newData);

            Assert.Equal(newData, File.ReadAllBytes(path));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── AtomicWriteAllText ───────────────────────────────────────────

    [Fact]
    public void AtomicWriteAllText_CreatesFile()
    {
        var dir = GetTempDir();
        try
        {
            var path = Path.Combine(dir, "test.txt");
            FileUtils.AtomicWriteAllText(path, "hello world");

            Assert.Equal("hello world", File.ReadAllText(path));
            Assert.False(File.Exists(path + ".tmp"));
        }
        finally { Directory.Delete(dir, true); }
    }

    [Fact]
    public void AtomicWriteAllText_OverwritesExistingFile()
    {
        var dir = GetTempDir();
        try
        {
            var path = Path.Combine(dir, "test.txt");
            File.WriteAllText(path, "old content");

            FileUtils.AtomicWriteAllText(path, "new content");

            Assert.Equal("new content", File.ReadAllText(path));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── AtomicWriteAllLines ──────────────────────────────────────────

    [Fact]
    public void AtomicWriteAllLines_CreatesFile()
    {
        var dir = GetTempDir();
        try
        {
            var path = Path.Combine(dir, "lines.txt");
            var lines = new[] { "line1", "line2", "line3" };

            FileUtils.AtomicWriteAllLines(path, lines);

            var written = File.ReadAllLines(path);
            Assert.Equal(lines, written);
            Assert.False(File.Exists(path + ".tmp"));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── FormatSize ───────────────────────────────────────────────────

    [Theory]
    [InlineData(0, "0 B")]
    [InlineData(512, "512 B")]
    [InlineData(1023, "1023 B")]
    [InlineData(1024, "1.0 KB")]
    [InlineData(1536, "1.5 KB")]
    [InlineData(1048576, "1.0 MB")]
    [InlineData(1572864, "1.5 MB")]
    [InlineData(1073741824, "1.0 GB")]
    public void FormatSize_ReturnsExpected(long bytes, string expected)
    {
        Assert.Equal(expected, FileUtils.FormatSize(bytes));
    }

    [Fact]
    public void FormatSize_LargeValue()
    {
        // 2.5 GB
        var result = FileUtils.FormatSize(2684354560);
        Assert.Equal("2.5 GB", result);
    }
}
