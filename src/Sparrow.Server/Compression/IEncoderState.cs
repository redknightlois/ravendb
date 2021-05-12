using System;

namespace Sparrow.Server.Compression
{ 
    public interface IEncoderState
    {
        Span<byte> EncodingTable { get; }
        Span<byte> DecodingTable { get; }
    }
}
