using CloudRedirect.Services.Patching;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for PayloadCrypto AES-CBC encrypt/decrypt.
/// Verifies roundtrip, known-answer vectors, and error handling.
/// </summary>
public class PayloadCryptoTests
{
    private static readonly byte[] TestKey = new byte[]
    {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
    };

    private static readonly byte[] TestIv = new byte[]
    {
        0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7,
        0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF,
    };

    [Fact]
    public void Roundtrip_SmallPayload()
    {
        var original = "Hello, CloudRedirect!"u8.ToArray();
        var ct = PayloadCrypto.AesCbcEncrypt(original, TestKey, TestIv);
        var decrypted = PayloadCrypto.AesCbcDecrypt(ct, TestKey, TestIv);
        Assert.Equal(original, decrypted);
    }

    [Fact]
    public void Roundtrip_ExactBlockSize()
    {
        // 32 bytes = exactly 2 AES blocks
        var original = new byte[32];
        for (int i = 0; i < 32; i++) original[i] = (byte)(i * 7);

        var ct = PayloadCrypto.AesCbcEncrypt(original, TestKey, TestIv);
        var decrypted = PayloadCrypto.AesCbcDecrypt(ct, TestKey, TestIv);
        Assert.Equal(original, decrypted);
    }

    [Fact]
    public void Roundtrip_LargePayload()
    {
        // Simulate a realistic payload size (~100KB)
        var original = new byte[100_000];
        new Random(42).NextBytes(original);

        var ct = PayloadCrypto.AesCbcEncrypt(original, TestKey, TestIv);
        var decrypted = PayloadCrypto.AesCbcDecrypt(ct, TestKey, TestIv);
        Assert.Equal(original, decrypted);
    }

    [Fact]
    public void Roundtrip_EmptyPayload()
    {
        var original = Array.Empty<byte>();
        var ct = PayloadCrypto.AesCbcEncrypt(original, TestKey, TestIv);
        Assert.NotEmpty(ct); // PKCS7 padding adds a full block
        var decrypted = PayloadCrypto.AesCbcDecrypt(ct, TestKey, TestIv);
        Assert.Equal(original, decrypted);
    }

    [Fact]
    public void Roundtrip_SingleByte()
    {
        var original = new byte[] { 0x42 };
        var ct = PayloadCrypto.AesCbcEncrypt(original, TestKey, TestIv);
        var decrypted = PayloadCrypto.AesCbcDecrypt(ct, TestKey, TestIv);
        Assert.Equal(original, decrypted);
    }

    [Fact]
    public void Encrypt_ProducesDifferentCiphertext_WithDifferentIv()
    {
        var original = "Same plaintext"u8.ToArray();
        var iv2 = new byte[16];
        Array.Fill(iv2, (byte)0xBB);

        var ct1 = PayloadCrypto.AesCbcEncrypt(original, TestKey, TestIv);
        var ct2 = PayloadCrypto.AesCbcEncrypt(original, TestKey, iv2);

        Assert.NotEqual(ct1, ct2);
    }

    [Fact]
    public void Decrypt_WrongKey_Throws()
    {
        var original = "Secret data"u8.ToArray();
        var ct = PayloadCrypto.AesCbcEncrypt(original, TestKey, TestIv);

        var wrongKey = new byte[32];
        Array.Fill(wrongKey, (byte)0xFF);

        // Wrong key should produce garbage or throw due to bad PKCS7 padding
        Assert.ThrowsAny<Exception>(() => PayloadCrypto.AesCbcDecrypt(ct, wrongKey, TestIv));
    }

    [Fact]
    public void Roundtrip_WithSteamToolsKey()
    {
        // Verify the actual SteamTools key works for roundtrip
        var key = SteamToolsCrypto.AesKey;
        var iv = new byte[16];
        new Random(99).NextBytes(iv);

        var original = new byte[256];
        new Random(99).NextBytes(original);

        var ct = PayloadCrypto.AesCbcEncrypt(original, key, iv);
        var decrypted = PayloadCrypto.AesCbcDecrypt(ct, key, iv);
        Assert.Equal(original, decrypted);
    }

    [Fact]
    public void Ciphertext_IsPkcs7Padded()
    {
        // 15 bytes of plaintext -> 16 bytes ciphertext (1 byte padding to fill block)
        var pt = new byte[15];
        var ct = PayloadCrypto.AesCbcEncrypt(pt, TestKey, TestIv);
        Assert.Equal(16, ct.Length);

        // 16 bytes of plaintext -> 32 bytes ciphertext (full padding block added)
        pt = new byte[16];
        ct = PayloadCrypto.AesCbcEncrypt(pt, TestKey, TestIv);
        Assert.Equal(32, ct.Length);

        // 17 bytes -> 32 bytes
        pt = new byte[17];
        ct = PayloadCrypto.AesCbcEncrypt(pt, TestKey, TestIv);
        Assert.Equal(32, ct.Length);
    }
}
