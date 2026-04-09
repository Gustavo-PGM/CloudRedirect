using System.Text.Json;
using CloudRedirect.Services;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for CloudCleanupRevert: restore from undo log, conflict modes, path
/// safety validation, and stale destPath resolution.
/// </summary>
public class CloudCleanupRevertTests
{
    private string GetTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "CloudRedirect_Tests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    /// <summary>
    /// Creates a fake Steam directory structure with a backup and undo log.
    /// Returns (steamPath, undoLogPath, backupFilePath).
    /// </summary>
    private (string steamPath, string undoLogPath, string backupFilePath) SetupBackup(
        string tempDir, string accountId, string backupId, string fileName,
        byte[] fileContent, uint appId, string? sourcePath = null)
    {
        var steamPath = tempDir;
        var backupDir = Path.Combine(steamPath, "cloud_redirect", BackupPaths.CleanupDir, accountId, backupId);
        Directory.CreateDirectory(backupDir);

        var backupFilePath = Path.Combine(backupDir, fileName);
        File.WriteAllBytes(backupFilePath, fileContent);

        sourcePath ??= Path.Combine(tempDir, "userdata", accountId, "remote", fileName);

        var log = new UndoLog
        {
            Timestamp = "2025-04-07T12:00:00Z",
            Version = 1,
            Operations = new()
            {
                new UndoOperation
                {
                    Type = "file_move",
                    SourcePath = sourcePath,
                    DestPath = backupFilePath,
                    AppId = appId
                }
            }
        };

        var undoLogPath = Path.Combine(backupDir, "undo_log.json");
        File.WriteAllText(undoLogPath, JsonSerializer.Serialize(log, CleanupJsonContext.Default.UndoLog));
        return (steamPath, undoLogPath, backupFilePath);
    }

    // ── Dry run ────────────────────────────────────────────────────────

    [Fact]
    public void RestoreFromLog_DryRun_DoesNotCreateFiles()
    {
        var dir = GetTempDir();
        try
        {
            var (steamPath, undoLogPath, _) = SetupBackup(dir, "12345678", "bk1", "save.dat",
                new byte[] { 0xAA }, 1221480);

            var revert = new CloudCleanupRevert(steamPath);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: true);

            Assert.Equal(1, result.FilesRestored);
            Assert.Equal(0, result.FilesSkipped);
            // Source file should NOT exist (dry run)
            var sourcePath = Path.Combine(dir, "userdata", "12345678", "remote", "save.dat");
            Assert.False(File.Exists(sourcePath));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Actual restore ─────────────────────────────────────────────────

    [Fact]
    public void RestoreFromLog_ActualRestore_CopiesFile()
    {
        var dir = GetTempDir();
        try
        {
            var sourceDir = Path.Combine(dir, "userdata", "12345678", "remote");
            Directory.CreateDirectory(sourceDir);
            var sourcePath = Path.Combine(sourceDir, "save.dat");

            var (steamPath, undoLogPath, _) = SetupBackup(dir, "12345678", "bk1", "save.dat",
                new byte[] { 0xBB, 0xCC }, 1221480, sourcePath);

            var revert = new CloudCleanupRevert(steamPath);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: false);

            Assert.Equal(1, result.FilesRestored);
            Assert.True(File.Exists(sourcePath));
            Assert.Equal(new byte[] { 0xBB, 0xCC }, File.ReadAllBytes(sourcePath));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Conflict mode: Skip ────────────────────────────────────────────

    [Fact]
    public void RestoreFromLog_ConflictSkip_LeavesExistingFile()
    {
        var dir = GetTempDir();
        try
        {
            var sourceDir = Path.Combine(dir, "userdata", "12345678", "remote");
            Directory.CreateDirectory(sourceDir);
            var sourcePath = Path.Combine(sourceDir, "save.dat");
            File.WriteAllBytes(sourcePath, new byte[] { 0x01 }); // existing file

            var (steamPath, undoLogPath, _) = SetupBackup(dir, "12345678", "bk1", "save.dat",
                new byte[] { 0x02 }, 1221480, sourcePath);

            var revert = new CloudCleanupRevert(steamPath, RevertConflictMode.Skip);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: false);

            Assert.Equal(0, result.FilesRestored);
            Assert.Equal(1, result.FilesSkipped);
            Assert.Equal(1, result.FilesConflict);
            // Original file unchanged
            Assert.Equal(new byte[] { 0x01 }, File.ReadAllBytes(sourcePath));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Conflict mode: Overwrite ───────────────────────────────────────

    [Fact]
    public void RestoreFromLog_ConflictOverwrite_ReplacesExistingFile()
    {
        var dir = GetTempDir();
        try
        {
            var sourceDir = Path.Combine(dir, "userdata", "12345678", "remote");
            Directory.CreateDirectory(sourceDir);
            var sourcePath = Path.Combine(sourceDir, "save.dat");
            File.WriteAllBytes(sourcePath, new byte[] { 0x01 }); // existing

            var (steamPath, undoLogPath, _) = SetupBackup(dir, "12345678", "bk1", "save.dat",
                new byte[] { 0x02, 0x03 }, 1221480, sourcePath);

            var revert = new CloudCleanupRevert(steamPath, RevertConflictMode.Overwrite);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: false);

            Assert.Equal(1, result.FilesRestored);
            Assert.Equal(1, result.FilesConflict);
            Assert.Equal(new byte[] { 0x02, 0x03 }, File.ReadAllBytes(sourcePath));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Conflict mode: Rename ──────────────────────────────────────────

    [Fact]
    public void RestoreFromLog_ConflictRename_BacksUpExistingFile()
    {
        var dir = GetTempDir();
        try
        {
            var sourceDir = Path.Combine(dir, "userdata", "12345678", "remote");
            Directory.CreateDirectory(sourceDir);
            var sourcePath = Path.Combine(sourceDir, "save.dat");
            File.WriteAllBytes(sourcePath, new byte[] { 0x01 }); // existing

            var (steamPath, undoLogPath, _) = SetupBackup(dir, "12345678", "bk1", "save.dat",
                new byte[] { 0x02, 0x03 }, 1221480, sourcePath);

            var revert = new CloudCleanupRevert(steamPath, RevertConflictMode.Rename);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: false);

            Assert.Equal(1, result.FilesRestored);
            Assert.Equal(1, result.FilesConflict);
            // Restored file has backup content
            Assert.Equal(new byte[] { 0x02, 0x03 }, File.ReadAllBytes(sourcePath));
            // Old file was renamed
            var bakPath = sourcePath + ".pre-revert.bak";
            Assert.True(File.Exists(bakPath));
            Assert.Equal(new byte[] { 0x01 }, File.ReadAllBytes(bakPath));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Missing backup file ────────────────────────────────────────────

    [Fact]
    public void RestoreFromLog_MissingBackupFile_SkipsOperation()
    {
        var dir = GetTempDir();
        try
        {
            var backupDir = Path.Combine(dir, "cloud_redirect", BackupPaths.CleanupDir, "12345678", "bk1");
            Directory.CreateDirectory(backupDir);

            // Create undo log pointing to a file that doesn't exist
            var log = new UndoLog
            {
                Timestamp = "2025-04-07T12:00:00Z",
                Version = 1,
                Operations = new()
                {
                    new UndoOperation
                    {
                        Type = "file_move",
                        SourcePath = Path.Combine(dir, "userdata", "save.dat"),
                        DestPath = Path.Combine(backupDir, "nonexistent.dat"),
                        AppId = 100
                    }
                }
            };

            var undoLogPath = Path.Combine(backupDir, "undo_log.json");
            File.WriteAllText(undoLogPath, JsonSerializer.Serialize(log, CleanupJsonContext.Default.UndoLog));

            var revert = new CloudCleanupRevert(dir);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: false);

            Assert.Equal(0, result.FilesRestored);
            Assert.Equal(1, result.FilesSkipped);
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Path safety: unsafe SourcePath ─────────────────────────────────

    [Fact]
    public void RestoreFromLog_UnsafeSourcePath_Skipped()
    {
        var dir = GetTempDir();
        try
        {
            var (steamPath, _, backupFile) = SetupBackup(dir, "12345678", "bk1", "save.dat",
                new byte[] { 0xAA }, 100);

            // Overwrite the undo log with an unsafe source path
            var backupDir = Path.GetDirectoryName(backupFile)!;
            var log = new UndoLog
            {
                Timestamp = "2025-04-07T12:00:00Z",
                Version = 1,
                Operations = new()
                {
                    new UndoOperation
                    {
                        Type = "file_move",
                        SourcePath = @"relative\path\save.dat", // not rooted
                        DestPath = backupFile,
                        AppId = 100
                    }
                }
            };
            File.WriteAllText(Path.Combine(backupDir, "undo_log.json"),
                JsonSerializer.Serialize(log, CleanupJsonContext.Default.UndoLog));

            var revert = new CloudCleanupRevert(steamPath);
            var result = revert.RestoreFromLog(
                Path.Combine(backupDir, "undo_log.json"), dryRun: false);

            Assert.Equal(0, result.FilesRestored);
            Assert.Equal(1, result.FilesSkipped);
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Path safety: path traversal ────────────────────────────────────

    [Fact]
    public void RestoreFromLog_PathTraversal_Skipped()
    {
        var dir = GetTempDir();
        try
        {
            var (steamPath, _, backupFile) = SetupBackup(dir, "12345678", "bk1", "save.dat",
                new byte[] { 0xAA }, 100);

            var backupDir = Path.GetDirectoryName(backupFile)!;
            var log = new UndoLog
            {
                Timestamp = "2025-04-07T12:00:00Z",
                Version = 1,
                Operations = new()
                {
                    new UndoOperation
                    {
                        Type = "file_move",
                        SourcePath = Path.Combine(dir, "..", "..", "etc", "passwd"),
                        DestPath = backupFile,
                        AppId = 100
                    }
                }
            };
            File.WriteAllText(Path.Combine(backupDir, "undo_log.json"),
                JsonSerializer.Serialize(log, CleanupJsonContext.Default.UndoLog));

            var revert = new CloudCleanupRevert(steamPath);
            var result = revert.RestoreFromLog(
                Path.Combine(backupDir, "undo_log.json"), dryRun: false);

            Assert.Equal(0, result.FilesRestored);
            Assert.Equal(1, result.FilesSkipped);
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Stale destPath resolution ──────────────────────────────────────

    [Fact]
    public void RestoreFromLog_StaleDestPath_ResolvesViaLegacyFallback()
    {
        var dir = GetTempDir();
        try
        {
            // File lives at cleanup_tab_backup/ but undo log references cleanup_backup/
            var realBackupDir = Path.Combine(dir, "cloud_redirect", BackupPaths.CleanupDir, "12345678", "bk1");
            Directory.CreateDirectory(realBackupDir);
            var realFile = Path.Combine(realBackupDir, "save.dat");
            File.WriteAllBytes(realFile, new byte[] { 0xDD, 0xEE });

            var staleDestPath = Path.Combine(dir, "cloud_redirect", BackupPaths.LegacyDir, "12345678", "bk1", "save.dat");
            var sourceDir = Path.Combine(dir, "userdata", "12345678");
            Directory.CreateDirectory(sourceDir);
            var sourcePath = Path.Combine(sourceDir, "save.dat");

            var log = new UndoLog
            {
                Timestamp = "2025-04-07T12:00:00Z",
                Version = 1,
                Operations = new()
                {
                    new UndoOperation
                    {
                        Type = "file_move",
                        SourcePath = sourcePath,
                        DestPath = staleDestPath,
                        AppId = 100
                    }
                }
            };

            var undoLogPath = Path.Combine(realBackupDir, "undo_log.json");
            File.WriteAllText(undoLogPath, JsonSerializer.Serialize(log, CleanupJsonContext.Default.UndoLog));

            var revert = new CloudCleanupRevert(dir);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: false);

            Assert.Equal(1, result.FilesRestored);
            Assert.True(File.Exists(sourcePath));
            Assert.Equal(new byte[] { 0xDD, 0xEE }, File.ReadAllBytes(sourcePath));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Empty undo log ─────────────────────────────────────────────────

    [Fact]
    public void RestoreFromLog_EmptyLog_NoErrors()
    {
        var dir = GetTempDir();
        try
        {
            var backupDir = Path.Combine(dir, "cloud_redirect", BackupPaths.CleanupDir, "12345678", "bk1");
            Directory.CreateDirectory(backupDir);

            var log = new UndoLog { Timestamp = "2025-04-07T12:00:00Z", Operations = new() };
            var undoLogPath = Path.Combine(backupDir, "undo_log.json");
            File.WriteAllText(undoLogPath, JsonSerializer.Serialize(log, CleanupJsonContext.Default.UndoLog));

            var revert = new CloudCleanupRevert(dir);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: false);

            Assert.Equal(0, result.FilesRestored);
            Assert.Equal(0, result.FilesSkipped);
            Assert.Empty(result.Errors);
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── dir_delete operation ───────────────────────────────────────────

    [Fact]
    public void RestoreFromLog_DirDelete_RecreatesDirectory()
    {
        var dir = GetTempDir();
        try
        {
            var backupDir = Path.Combine(dir, "cloud_redirect", BackupPaths.CleanupDir, "12345678", "bk1");
            Directory.CreateDirectory(backupDir);

            var dirToRecreate = Path.Combine(dir, "userdata", "12345678", "1221480");

            var log = new UndoLog
            {
                Timestamp = "2025-04-07T12:00:00Z",
                Version = 1,
                Operations = new()
                {
                    new UndoOperation { Type = "dir_delete", SourcePath = dirToRecreate, AppId = 1221480 }
                }
            };

            var undoLogPath = Path.Combine(backupDir, "undo_log.json");
            File.WriteAllText(undoLogPath, JsonSerializer.Serialize(log, CleanupJsonContext.Default.UndoLog));

            var revert = new CloudCleanupRevert(dir);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: false);

            Assert.Equal(1, result.DirsRecreated);
            Assert.True(Directory.Exists(dirToRecreate));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── remotecache_backup operation ───────────────────────────────────

    [Fact]
    public void RestoreFromLog_RemotecacheBackup_RestoresContent()
    {
        var dir = GetTempDir();
        try
        {
            var backupDir = Path.Combine(dir, "cloud_redirect", BackupPaths.CleanupDir, "12345678", "bk1");
            Directory.CreateDirectory(backupDir);

            var remotecachePath = Path.Combine(dir, "userdata", "12345678", "1221480", "remotecache.vdf");
            Directory.CreateDirectory(Path.GetDirectoryName(remotecachePath)!);

            var log = new UndoLog
            {
                Timestamp = "2025-04-07T12:00:00Z",
                Version = 1,
                Operations = new()
                {
                    new UndoOperation
                    {
                        Type = "remotecache_backup",
                        SourcePath = remotecachePath,
                        BackupContent = "\"remotecache\" { }",
                        AppId = 1221480
                    }
                }
            };

            var undoLogPath = Path.Combine(backupDir, "undo_log.json");
            File.WriteAllText(undoLogPath, JsonSerializer.Serialize(log, CleanupJsonContext.Default.UndoLog));

            var revert = new CloudCleanupRevert(dir);
            var result = revert.RestoreFromLog(undoLogPath, dryRun: false);

            Assert.Equal(1, result.RemotecachesRestored);
            Assert.True(File.Exists(remotecachePath));
            Assert.Equal("\"remotecache\" { }", File.ReadAllText(remotecachePath));
        }
        finally { Directory.Delete(dir, true); }
    }

    // ── Logging ────────────────────────────────────────────────────────

    [Fact]
    public void RestoreFromLog_LogCallbackReceivesMessages()
    {
        var dir = GetTempDir();
        try
        {
            var (steamPath, undoLogPath, _) = SetupBackup(dir, "12345678", "bk1", "save.dat",
                new byte[] { 0xAA }, 100);

            var messages = new List<string>();
            var revert = new CloudCleanupRevert(steamPath, log: msg => messages.Add(msg));
            revert.RestoreFromLog(undoLogPath, dryRun: true);

            Assert.True(messages.Count > 0);
            Assert.Contains(messages, m => m.Contains("Restoring from Backup"));
        }
        finally { Directory.Delete(dir, true); }
    }
}
