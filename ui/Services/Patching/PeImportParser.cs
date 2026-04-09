using System;

namespace CloudRedirect.Services.Patching
{
    /// <summary>
    /// Parses PE import directory tables to locate IAT entries.
    /// Extracted from Patcher -- used to find LoadLibraryA and GetProcAddress
    /// IAT slots for the CloudRedirect code cave.
    /// </summary>
    internal static class PeImportParser
    {
        /// <summary>
        /// Read a null-terminated ASCII string from a byte array.
        /// </summary>
        public static string ReadAsciiZ(byte[] data, int offset)
        {
            int end = offset;
            while (end < data.Length && data[end] != 0) end++;
            if (end == offset) return string.Empty;
            return System.Text.Encoding.ASCII.GetString(data, offset, end - offset);
        }

        /// <summary>
        /// Find LoadLibraryA and GetProcAddress IAT entry RVAs from the PE import directory.
        /// Returns (-1, -1) if the entries cannot be found.
        /// </summary>
        public static (int loadLibA, int getProcAddr) FindKernel32IatEntries(byte[] pe, PeSection[] sections)
        {
            if (pe.Length < 64) return (-1, -1);

            int peOff = BitConverter.ToInt32(pe, 0x3C);
            if (peOff < 0 || peOff + 24 > pe.Length) return (-1, -1);

            int magic = BitConverter.ToUInt16(pe, peOff + 24);
            int importDirOffset;
            if (magic == 0x20B) // PE32+
                importDirOffset = peOff + 24 + 120;
            else if (magic == 0x10B) // PE32
                importDirOffset = peOff + 24 + 104;
            else
                return (-1, -1);

            if (importDirOffset + 8 > pe.Length) return (-1, -1);

            int importRva = BitConverter.ToInt32(pe, importDirOffset);
            int importSize = BitConverter.ToInt32(pe, importDirOffset + 4);
            if (importRva == 0 || importSize == 0) return (-1, -1);

            int importFileOff = PeSection.RvaToFileOffset(sections, importRva);
            if (importFileOff < 0) return (-1, -1);

            int loadLibA = -1, getProcAddr = -1;

            // walk import descriptors (20 bytes each)
            for (int desc = importFileOff; desc + 20 <= pe.Length; desc += 20)
            {
                int nameRva = BitConverter.ToInt32(pe, desc + 12);
                if (nameRva == 0) break;

                int nameOff = PeSection.RvaToFileOffset(sections, nameRva);
                if (nameOff < 0 || nameOff >= pe.Length) continue;

                string dllName = ReadAsciiZ(pe, nameOff);
                if (!dllName.Equals("KERNEL32.dll", StringComparison.OrdinalIgnoreCase) &&
                    !dllName.Equals("kernel32.dll", StringComparison.OrdinalIgnoreCase))
                    continue;

                int oftRva = BitConverter.ToInt32(pe, desc);
                int ftRva = BitConverter.ToInt32(pe, desc + 16);

                if (oftRva == 0) oftRva = ftRva;

                int oftOff = PeSection.RvaToFileOffset(sections, oftRva);
                int ftOff = PeSection.RvaToFileOffset(sections, ftRva);
                if (oftOff < 0 || ftOff < 0) continue;

                int thunkSize = (magic == 0x20B) ? 8 : 4;

                for (int ti = 0; ; ti++)
                {
                    int intEntryOff = oftOff + ti * thunkSize;
                    int iatEntryOff = ftOff + ti * thunkSize;

                    if (intEntryOff + thunkSize > pe.Length || iatEntryOff + thunkSize > pe.Length) break;

                    long thunkVal;
                    if (thunkSize == 8)
                        thunkVal = BitConverter.ToInt64(pe, intEntryOff);
                    else
                        thunkVal = BitConverter.ToUInt32(pe, intEntryOff);

                    if (thunkVal == 0) break;

                    bool isOrdinal = (thunkSize == 8)
                        ? (thunkVal & unchecked((long)0x8000000000000000)) != 0
                        : (thunkVal & 0x80000000) != 0;

                    if (isOrdinal) continue;

                    int hintNameRva = (int)(thunkVal & 0xFFFFFFFF);
                    int hintNameOff = PeSection.RvaToFileOffset(sections, hintNameRva);
                    if (hintNameOff < 0 || hintNameOff + 2 >= pe.Length) continue;

                    string funcName = ReadAsciiZ(pe, hintNameOff + 2);

                    int iatEntryRva = ftRva + ti * thunkSize;

                    if (funcName == "LoadLibraryA")
                        loadLibA = iatEntryRva;
                    else if (funcName == "GetProcAddress")
                        getProcAddr = iatEntryRva;

                    if (loadLibA >= 0 && getProcAddr >= 0)
                        return (loadLibA, getProcAddr);
                }

                break; // only process KERNEL32
            }

            return (loadLibA, getProcAddr);
        }
    }
}
