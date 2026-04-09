using System.Security.Cryptography;

namespace CloudRedirect.Services.Patching
{
    /// <summary>
    /// AES-CBC encrypt/decrypt helpers shared by Patcher and Fingerprint.
    /// Uses the SteamTools-compatible key from <see cref="SteamToolsCrypto"/>.
    /// </summary>
    internal static class PayloadCrypto
    {
        public static byte[] AesCbcDecrypt(byte[] ct, byte[] key, byte[] iv)
        {
            using var aes = Aes.Create();
            aes.Key = key; aes.IV = iv;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            using var dec = aes.CreateDecryptor();
            return dec.TransformFinalBlock(ct, 0, ct.Length);
        }

        public static byte[] AesCbcEncrypt(byte[] pt, byte[] key, byte[] iv)
        {
            using var aes = Aes.Create();
            aes.Key = key; aes.IV = iv;
            aes.Mode = CipherMode.CBC;
            aes.Padding = PaddingMode.PKCS7;
            using var enc = aes.CreateEncryptor();
            return enc.TransformFinalBlock(pt, 0, pt.Length);
        }
    }
}
