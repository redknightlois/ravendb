using System;

namespace Sparrow.Server.Compression
{ 
    public interface IEncoderState
    {
        Span<byte> Table { get; }
    }
}
