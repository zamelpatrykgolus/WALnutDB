using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

/// <summary>
/// Referencyjna implementacja AEAD: AES-GCM(256) z AAD=[table|pk].
/// Format: [ver:1]=0x01 | [nonce:12] | [tag:16] | [cipher:N]
/// </summary>
public sealed class AesGcmEncryption : IEncryption, IDisposable
{
    private readonly AesGcm _aes;
    private readonly RandomNumberGenerator _rng = RandomNumberGenerator.Create();

    public AesGcmEncryption(ReadOnlySpan<byte> key256)
        => _aes = new AesGcm(key256.ToArray(), tagSizeInBytes: 16); // 16/24/32B – zalecam 32B

    public byte[] Encrypt(ReadOnlySpan<byte> plaintext, string table, ReadOnlySpan<byte> pk)
    {
        var nonce = new byte[12];
        _rng.GetBytes(nonce);

        var aad = BuildAad(table, pk);

        var cipher = new byte[plaintext.Length];
        var tag = new byte[16];

        _aes.Encrypt(nonce, plaintext, cipher, tag, aad);

        var result = new byte[1 + nonce.Length + tag.Length + cipher.Length];
        result[0] = 0x01; // wersja formatu
        Buffer.BlockCopy(nonce, 0, result, 1, nonce.Length);
        Buffer.BlockCopy(tag, 0, result, 1 + nonce.Length, tag.Length);
        Buffer.BlockCopy(cipher, 0, result, 1 + nonce.Length + tag.Length, cipher.Length);
        return result;
    }

    public byte[] Decrypt(ReadOnlySpan<byte> ciphertext, string table, ReadOnlySpan<byte> pk)
    {
        if (ciphertext.Length < 1 + 12 + 16)
            throw new InvalidDataException("Encrypted value too short.");

        byte ver = ciphertext[0];
        if (ver != 0x01)
            throw new InvalidDataException($"Unsupported ciphertext version: {ver}.");

        var nonce = ciphertext.Slice(1, 12);
        var tag = ciphertext.Slice(13, 16);
        var cipher = ciphertext.Slice(29);

        var aad = BuildAad(table, pk);

        var plain = new byte[cipher.Length];
        _aes.Decrypt(nonce.ToArray(), cipher.ToArray(), tag.ToArray(), plain, aad);
        return plain;
    }

    private static byte[] BuildAad(string table, ReadOnlySpan<byte> pk)
    {
        var t = Encoding.UTF8.GetBytes(table);
        var aad = new byte[2 + t.Length + pk.Length];
        BinaryPrimitives.WriteUInt16LittleEndian(aad.AsSpan(0, 2), (ushort)t.Length);
        Buffer.BlockCopy(t, 0, aad, 2, t.Length);
        pk.CopyTo(aad.AsSpan(2 + t.Length));
        return aad;
    }

    public void Dispose()
    {
        _aes.Dispose();
        _rng.Dispose();
    }
}
