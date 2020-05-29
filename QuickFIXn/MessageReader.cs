using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace QuickFix
{
    public class MessageReader
    {
        private const int MinBufferSize = 512;
        private const int SizeOfCheckSumValue = 4;
        private static readonly Memory<byte> MessageDelimiterMemory;
        private readonly Pipe _pipe;

        static MessageReader()
        {
            MessageDelimiterMemory = CharEncoding.DefaultEncoding.GetBytes('\x01' + "10=").AsMemory();
        }

        public MessageReader()
        {
            _pipe = new Pipe();
        }

        public async Task ReadStreamData(Stream stream, CancellationToken cancellationToken)
        {
            await Task.Yield();
            while (!cancellationToken.IsCancellationRequested)
            {
                var memory = _pipe.Writer.GetMemory(MinBufferSize);
                try
                {
                    var arraySegment = GetArray(memory);
                    var readCount = await stream.ReadAsync(arraySegment.Array, arraySegment.Offset,
                        arraySegment.Count, cancellationToken).ConfigureAwait(false);
                    if (readCount == 0) continue;
                    _pipe.Writer.Advance(readCount);
                }
                catch
                {
                    await _pipe.Writer.CompleteAsync().ConfigureAwait(false);
                    throw;
                }
                await _pipe.Writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private IEnumerator<byte> GetEnumerator(ReadOnlySequence<byte> buffer)
        {
            foreach (var segment in buffer)
            {
                for (var index = 0; index < segment.Span.Length; index++)
                {
                    var byteData = segment.Span[index];
                    yield return byteData;
                }
            }
        }

        public async Task ReadMessages(Func<string, CancellationToken, Task> receivedMessage, CancellationToken cancellationToken)
        {
            await Task.Yield();
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = await _pipe.Reader.ReadAsync(cancellationToken);
                    var buffer = result.Buffer;
                    var endOfMessagePosition = FindEndOfMessagePosition(buffer);

                    while (endOfMessagePosition.HasValue)
                    {
                        var msg = buffer.Slice(0, endOfMessagePosition.Value);
                        await receivedMessage(GetString(msg), cancellationToken);
                        buffer = buffer.Slice(endOfMessagePosition.Value);
                        endOfMessagePosition = FindEndOfMessagePosition(buffer);
                    }
                    _pipe.Reader.AdvanceTo(buffer.Start, buffer.End);
                }
            }
            finally
            {
                await _pipe.Reader.CompleteAsync().ConfigureAwait(false);
            }
        }

        private ArraySegment<byte> GetArray(ReadOnlyMemory<byte> memory)
        {
            if (!MemoryMarshal.TryGetArray(memory, out var result)) throw new InvalidOperationException("Buffer backed by array was expected");
            return result;
        }

        private string GetString(ReadOnlySequence<byte> memory)
        {
            //var arraySegment = GetArray(memory);
            //return CharEncoding.DefaultEncoding.GetString(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
            return CharEncoding.DefaultEncoding.GetString(memory.ToArray());
        }

        private SequencePosition? FindEndOfMessagePosition(ReadOnlySequence<byte> buffer)
        {
            var delimiter = MessageDelimiterMemory.Span;
            var position = buffer.PositionOf(delimiter[0]);
            var tempBuffer = buffer;
            do
            {
                if (!position.HasValue) return default;
                tempBuffer = tempBuffer.Slice(position.Value);

                using (var iterator = GetEnumerator(tempBuffer))
                {
                    for (var i = 0; i < delimiter.Length; i++)
                    {
                        if (!iterator.MoveNext() || iterator.Current != delimiter[i]) break;
                        if (i != delimiter.Length - 1) continue;
                        var pos = position.Value.GetInteger() - tempBuffer.Start.GetInteger();
                        var endOffset = pos + delimiter.Length + SizeOfCheckSumValue;
                        return endOffset <= tempBuffer.Length ? tempBuffer.GetPosition(endOffset) : default(SequencePosition?);
                    }
                }

                tempBuffer = tempBuffer.Slice(1);
                position = tempBuffer.PositionOf(delimiter[0]);
            } while (position.HasValue);
            return default;
        }
    }
}
