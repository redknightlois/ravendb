using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Binary;

namespace Sparrow.Json
{
    public sealed class AsyncBlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IDisposable, IAsyncDisposable
    {
        private readonly MemoryStream _inner;
        private readonly Stream _outputStream;
        private readonly CancellationToken _cancellationToken;

        public long BufferCapacity => _inner.Capacity;
        public long BufferUsed => _inner.Length;

        public AsyncBlittableJsonTextWriter(JsonOperationContext context, Stream stream, CancellationToken cancellationToken = default) : base(context, context.CheckoutMemoryStream())
        {
            _outputStream = stream ?? throw new ArgumentNullException(nameof(stream));
            _cancellationToken = cancellationToken;

            if (_stream is not MemoryStream)
                ThrowInvalidTypeException(_stream?.GetType());
            _inner = (MemoryStream)_stream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> MaybeOuterFlushAsync()
        {
            if (_inner.Length * 2 <= _inner.Capacity)
                return new ValueTask<int>(0);

            FlushInternal();
            return new ValueTask<int>(OuterFlushAsync());
        }

        public async Task<int> OuterFlushAsync()
        {
            FlushInternal();
            _inner.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount == 0)
                return 0;
            await _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount, _cancellationToken).ConfigureAwait(false);
            _inner.SetLength(0);
            return bytesCount;
        }

        public async ValueTask WriteStreamAsync(Stream stream, CancellationToken token = default)
        {
            await FlushAsync(token).ConfigureAwait(false);

            while (true)
            {
                _pos = await stream.ReadAsync(_pinnedBuffer.Memory.Memory, token).ConfigureAwait(false);
                if (_pos == 0)
                    break;

                await FlushAsync(token).ConfigureAwait(false);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> MaybeFlushAsync(CancellationToken token = default)
        {
            if (_inner.Length * 2 <= _inner.Capacity)
                return new ValueTask<int>(0);

            FlushInternal(); // this is OK, because inner stream is a MemoryStream
            return FlushAsync(token);
        }

        public async ValueTask<int> FlushAsync(CancellationToken token = default)
        {
            FlushInternal();

            _inner.TryGetBuffer(out var bytes);

            var bytesCount = bytes.Count;
            if (bytesCount == 0)
                return 0;

            var flushTask = _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount, token);
            if (flushTask.IsCompleted == false)
                flushTask.RunSynchronously();

            _outputStream.FlushAsync().ConfigureAwait(false);

            _requiresFlushOnDispose = false;

            _inner.SetLength(0);
            return bytesCount;
        }

        private void ThrowInvalidTypeException(Type typeOfStream)
        {
            throw new ArgumentException($"Expected stream to be {nameof(MemoryStream)}, but got {(typeOfStream == null ? "null" : typeOfStream.ToString())}.");
        }

        public async ValueTask DisposeAsync()
        {
            bool needToFlushOutputStream = _pos != 0 && _requiresFlushOnDispose;

            // Ensure we flush the internal auxiliary buffer.
            DisposeInternal();

            // Now e will try to get the data. 
            _inner.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount != 0)
            {
                var flushTask = _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount);
                if (flushTask.IsCompleted == false)
                    flushTask.RunSynchronously();

                needToFlushOutputStream = true;
            }

            if (needToFlushOutputStream)
            {
                var flushTask = _outputStream.FlushAsync();
                if (flushTask.IsCompleted == false)
                    flushTask.RunSynchronously();

                _requiresFlushOnDispose = false;
            }

            _context.ReturnMemoryStream(_inner);
        }

        public void Dispose()
        {
            bool needToFlushOutputStream = _pos != 0 && _requiresFlushOnDispose;

            // Ensure we flush the internal auxiliary buffer.
            DisposeInternal();

            // Now e will try to get the data. 
            _inner.TryGetBuffer(out var bytes);
            var bytesCount = bytes.Count;
            if (bytesCount != 0)
            {
                var flushTask = _outputStream.WriteAsync(bytes.Array, bytes.Offset, bytesCount);
                if (flushTask.IsCompleted == false)
                    flushTask.RunSynchronously();

                needToFlushOutputStream = true;
            }

            if (needToFlushOutputStream)
            {
                var flushTask = _outputStream.FlushAsync();
                if (flushTask.IsCompleted == false)
                    flushTask.RunSynchronously();

                _requiresFlushOnDispose = false;
            }

            _context.ReturnMemoryStream(_inner);
        }
    }
}
