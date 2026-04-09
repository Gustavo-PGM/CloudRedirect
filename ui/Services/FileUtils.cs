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

        public static string FormatSize(long bytes)
        {
            if (bytes < 1024) return $"{bytes} B";
            if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
            if (bytes < 1024L * 1024 * 1024) return $"{bytes / (1024.0 * 1024.0):F1} MB";
            return $"{bytes / (1024.0 * 1024.0 * 1024.0):F1} GB";
        }
    }
}
