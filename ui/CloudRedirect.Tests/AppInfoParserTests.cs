using CloudRedirect.Services;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for AppInfoParser: binary VDF v29 parsing, string table handling,
/// KV tree navigation, and AutoCloud rule extraction.
/// Constructs synthetic appinfo.vdf binaries to exercise the parser.
/// </summary>
public class AppInfoParserTests
{
    // ── Binary VDF builder helpers ──────────────────────────────────

    /// <summary>
    /// Builds a minimal appinfo.vdf v29 binary with a single app record
    /// that has a UFS section containing the specified AutoCloud rules.
    /// </summary>
    static byte[] BuildAppInfoVdf(uint appId, int quota, int maxFiles, params (string root, string path, string pattern, bool recursive)[] rules)
    {
        // Strategy:
        // 1. Build the string table containing all key names we use
        // 2. Build the KV binary data for the app record
        // 3. Build the app payload (40-byte header + 20-byte SHA1 + KV data)
        // 4. Assemble: 16-byte header + app record(s) + 0x00000000 sentinel + string table

        // String table entries (we use indices into this table for KV keys)
        var strings = new List<string>
        {
            "appinfo",      // 0
            "ufs",          // 1
            "quota",        // 2
            "maxnumfiles",  // 3
            "savefiles",    // 4
            "root",         // 5
            "path",         // 6
            "pattern",      // 7
            "recursive",    // 8
        };

        // Add entry indices for savefiles ("0", "1", "2", ...)
        for (int i = 0; i < rules.Length; i++)
        {
            strings.Add(i.ToString()); // indices 9, 10, ...
        }

        // Build the KV binary
        var kv = new MemoryStream();
        void WriteKvSection(uint keyIdx)
        {
            kv.WriteByte(0x00); // KV_SECTION
            kv.Write(BitConverter.GetBytes(keyIdx));
        }
        void WriteKvString(uint keyIdx, string value)
        {
            kv.WriteByte(0x01); // KV_STRING
            kv.Write(BitConverter.GetBytes(keyIdx));
            kv.Write(System.Text.Encoding.UTF8.GetBytes(value));
            kv.WriteByte(0x00); // null terminator
        }
        void WriteKvInt32(uint keyIdx, int value)
        {
            kv.WriteByte(0x02); // KV_INT32
            kv.Write(BitConverter.GetBytes(keyIdx));
            kv.Write(BitConverter.GetBytes(value));
        }
        void WriteKvEnd()
        {
            kv.WriteByte(0x08); // KV_END
        }

        // Root: appinfo section
        WriteKvSection(0); // "appinfo"
        {
            // ufs section
            WriteKvSection(1); // "ufs"
            {
                WriteKvInt32(2, quota);        // "quota"
                WriteKvInt32(3, maxFiles);     // "maxnumfiles"

                // savefiles section
                WriteKvSection(4); // "savefiles"
                {
                    for (int i = 0; i < rules.Length; i++)
                    {
                        uint entryIdx = (uint)(9 + i);
                        WriteKvSection(entryIdx); // "0", "1", etc.
                        {
                            WriteKvString(5, rules[i].root);      // "root"
                            WriteKvString(6, rules[i].path);      // "path"
                            WriteKvString(7, rules[i].pattern);   // "pattern"
                            WriteKvInt32(8, rules[i].recursive ? 1 : 0); // "recursive"
                            WriteKvEnd();
                        }
                    }
                    WriteKvEnd(); // end savefiles
                }
                WriteKvEnd(); // end ufs
            }
            WriteKvEnd(); // end appinfo
        }
        WriteKvEnd(); // end root

        byte[] kvData = kv.ToArray();

        // Build app payload: 40-byte header + 20-byte SHA1 + KV data
        byte[] payload = new byte[60 + kvData.Length];
        // Header and SHA1 can be zeros -- the parser only skips them
        Array.Copy(kvData, 0, payload, 60, kvData.Length);

        // Now assemble the full file
        var file = new MemoryStream();
        var writer = new BinaryWriter(file, System.Text.Encoding.UTF8);

        // 16-byte header
        writer.Write(0x07564429u); // magic v29
        writer.Write(1u);          // universe
        // string table offset -- we'll fill this in later
        long stOffsetPos = file.Position;
        writer.Write(0UL); // placeholder

        // App record: appId(4) + size(4) + payload
        writer.Write(appId);
        writer.Write((uint)payload.Length);
        writer.Write(payload);

        // End sentinel
        writer.Write(0u);

        // String table starts here
        long stringTableOffset = file.Position;

        // Write string table: count(4) + null-terminated strings
        writer.Write((uint)strings.Count);
        foreach (var s in strings)
        {
            writer.Write(System.Text.Encoding.UTF8.GetBytes(s));
            writer.Write((byte)0);
        }

        // Patch the string table offset in the header
        file.Seek(stOffsetPos, SeekOrigin.Begin);
        writer.Write((ulong)stringTableOffset);

        return file.ToArray();
    }

    static string WriteTempFile(byte[] data)
    {
        var path = Path.Combine(Path.GetTempPath(), $"appinfo_test_{Guid.NewGuid():N}.vdf");
        File.WriteAllBytes(path, data);
        return path;
    }

    // ── ParseAll ─────────────────────────────────────────────────────

    [Fact]
    public void ParseAll_SingleApp_ExtractsRules()
    {
        var data = BuildAppInfoVdf(480, 1048576, 500,
            ("gameinstall", "Saves", "*.sav", true),
            ("WinMyDocuments", "Config", "settings.json", false));

        var path = WriteTempFile(data);
        try
        {
            var results = AppInfoParser.ParseAll(path);
            Assert.Single(results);
            Assert.True(results.ContainsKey(480));

            var config = results[480];
            Assert.Equal(480u, config.AppId);
            Assert.Equal(1048576, config.Quota);
            Assert.Equal(500, config.MaxNumFiles);
            Assert.Equal(2, config.SaveFiles.Count);

            Assert.Equal("gameinstall", config.SaveFiles[0].Root);
            Assert.Equal("Saves", config.SaveFiles[0].Path);
            Assert.Equal("*.sav", config.SaveFiles[0].Pattern);
            Assert.True(config.SaveFiles[0].Recursive);

            Assert.Equal("WinMyDocuments", config.SaveFiles[1].Root);
            Assert.Equal("Config", config.SaveFiles[1].Path);
            Assert.Equal("settings.json", config.SaveFiles[1].Pattern);
            Assert.False(config.SaveFiles[1].Recursive);
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ── ParseSingle ──────────────────────────────────────────────────

    [Fact]
    public void ParseSingle_MatchingAppId_ReturnsConfig()
    {
        var data = BuildAppInfoVdf(730, 52428800, 1000,
            ("gameinstall", "", "*.cfg", false));

        var path = WriteTempFile(data);
        try
        {
            var config = AppInfoParser.ParseSingle(path, 730);
            Assert.NotNull(config);
            Assert.Equal(730u, config.AppId);
            Assert.Equal(52428800, config.Quota);
            Assert.Equal(1000, config.MaxNumFiles);
            Assert.Single(config.SaveFiles);
            Assert.Equal("gameinstall", config.SaveFiles[0].Root);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void ParseSingle_NonMatchingAppId_ReturnsNull()
    {
        var data = BuildAppInfoVdf(730, 0, 0,
            ("gameinstall", "", "*", false));

        var path = WriteTempFile(data);
        try
        {
            var config = AppInfoParser.ParseSingle(path, 999);
            Assert.Null(config);
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ── ParseAll with bad magic ──────────────────────────────────────

    [Fact]
    public void ParseAll_BadMagic_Throws()
    {
        var data = new byte[32];
        BitConverter.TryWriteBytes(data.AsSpan(0, 4), 0xDEADBEEFu);

        var path = WriteTempFile(data);
        try
        {
            Assert.Throws<InvalidDataException>(() => AppInfoParser.ParseAll(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void ParseSingle_BadMagic_Throws()
    {
        var data = new byte[32];
        BitConverter.TryWriteBytes(data.AsSpan(0, 4), 0xDEADBEEFu);

        var path = WriteTempFile(data);
        try
        {
            Assert.Throws<InvalidDataException>(() => AppInfoParser.ParseSingle(path, 1));
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ── Zero savefiles (app without AutoCloud) ───────────────────────

    [Fact]
    public void ParseAll_NoSaveFiles_ExcludesApp()
    {
        // Build an app with zero rules -- it should be excluded from ParseAll results
        var data = BuildAppInfoVdf(12345, 0, 0);

        var path = WriteTempFile(data);
        try
        {
            var results = AppInfoParser.ParseAll(path);
            Assert.Empty(results); // no apps with savefiles
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ── Quota and MaxNumFiles ────────────────────────────────────────

    [Fact]
    public void ParseSingle_QuotaAndMaxFiles_Extracted()
    {
        var data = BuildAppInfoVdf(42, 999999, 42,
            ("", "saves", "*", false));

        var path = WriteTempFile(data);
        try
        {
            var config = AppInfoParser.ParseSingle(path, 42);
            Assert.NotNull(config);
            Assert.Equal(999999, config.Quota);
            Assert.Equal(42, config.MaxNumFiles);
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ── Real appinfo.vdf (integration, skipped if file doesn't exist) ──

    [Fact]
    public void ParseAll_RealAppInfo_DoesNotThrow()
    {
        var realPath = @"C:\Games\Steam\appcache\appinfo.vdf";
        if (!File.Exists(realPath))
            return; // skip on machines without Steam

        // Should not throw and should return a non-empty dictionary
        var results = AppInfoParser.ParseAll(realPath);
        Assert.NotEmpty(results);

        // Spot check: every returned config should have at least one savefile
        foreach (var (appId, config) in results)
        {
            Assert.Equal(appId, config.AppId);
            Assert.NotEmpty(config.SaveFiles);
        }
    }

    [Fact]
    public void ParseSingle_RealAppInfo_Spacewar480()
    {
        var realPath = @"C:\Games\Steam\appcache\appinfo.vdf";
        if (!File.Exists(realPath))
            return; // skip on machines without Steam

        // Spacewar (480) is a well-known test app that should have UFS config
        var config = AppInfoParser.ParseSingle(realPath, 480);
        // Spacewar may or may not have AutoCloud -- just verify no crash
        // (it actually doesn't have savefiles, so config may be null)
    }
}
