using System.Linq;
using Sparrow.Platform;
using Voron;
using Xunit;

namespace FastTests.Voron.Graphs;

// The code tests on an encrypted environment, whose read transactions decrypt each page they touch into a buffer of their own.
public class HnswPrescreenCodesEncrypted(ITestOutputHelper output) : HnswPrescreenCodes(output)
{
    private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

    protected override void Configure(StorageEnvironmentOptions options)
    {
        base.Configure(options);
        options.Encryption.MasterKey = _masterKey.ToArray();
    }
}
