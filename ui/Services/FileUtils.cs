using System.Collections.Generic;
using System.IO;

namespace CloudRedirect.Services
{
    internal static class FileUtils
    {
        public static void AtomicWriteAllBytes(string path, byte[] data)
        {
            var tmp = path + ".tmp";
            File.WriteAllBytes(tmp, data);
            try { File.Move(tmp, path, overwrite: true); }
            catch { try { File.Delete(tmp); } catch { } throw; }
        }

        public static void AtomicWriteAllText(string path, string content)
        {
            var tmp = path + ".tmp";
            File.WriteAllText(tmp, content);
            try { File.Move(tmp, path, overwrite: true); }
            catch { try { File.Delete(tmp); } catch { } throw; }
        }

        public static void AtomicWriteAllLines(string path, IEnumerable<string> lines)
        {
            var tmp = path + ".tmp";
            File.WriteAllLines(tmp, lines);
            try { File.Move(tmp, path, overwrite: true); }
            catch { try { File.Delete(tmp); } catch { } throw; }
        }

        // Copies sourcePath to destPath atomically: writes a sibling .tmp first,
        // then File.Move(overwrite:true) which is rename-on-NTFS. A process kill
        // mid-copy leaves the .tmp orphan but never a torn destPath. Used by the
        // patcher Backup helper so an interrupted backup cannot leave a fragment
        // that a later Restore would overwrite a working binary with.
        public static void AtomicCopy(string sourcePath, string destPath)
        {
            var tmp = destPath + ".tmp";
            File.Copy(sourcePath, tmp, overwrite: true);
            try { File.Move(tmp, destPath, overwrite: true); }
            catch { try { File.Delete(tmp); } catch { } throw; }
        }

        public static string FormatSize(long bytes)
        {
            if (bytes < 1024) return $"{bytes} B";
            if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
            if (bytes < 1024L * 1024 * 1024) return $"{bytes / (1024.0 * 1024.0):F1} MB";
            return $"{bytes / (1024.0 * 1024.0 * 1024.0):F1} GB";
        }
    }
}
