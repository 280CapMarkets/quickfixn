using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuickFix;

namespace UnitTests
{
    [TestFixture]
    public class MessageReaderTests
    {
        private readonly MessageReader _target;
        public MessageReaderTests()
        {
            _target = new MessageReader();
        }

        [Test]
        public async Task ReadMessages_should_parse_1M_chanked_messages()
        {
            //arrange
            var msg =
                "8=FIXT.1.19=41935=634=675049=MA52=20200210-15:25:06.69056=280AXEOT1128=923=1054893731-0+128=R26=1054893731-0+055=[N/A]48=79642BXT522=1454=1455=US79642BXT50456=4167=NONE762=MUNI54=227=20000015=USD232=1233=MINQTY234=5000423=144=112.796640=112.7960=20200210-15:24:59236=1.0453=1448=XrefID-33447=D452=75961=MNPrice22022=120,300,900,180020120=ClientAxe-Orders20014=0.05620015=0.02829735=1054893731-0+010=019";
            
           
            var receivedMessageCount = 0;
            var expectedMessageCount = 1000000;
            using var cancellationTokenSource = new CancellationTokenSource();
            using var stream = new ProducerConsumerStream();

            //act
            var readDataFromStreamTask = _target.ReadStreamData(stream, cancellationTokenSource.Token);
            var readMessageTask = _target.ReadMessages((m, ct) =>
            {
                receivedMessageCount++;
                if (receivedMessageCount == expectedMessageCount)
                {
                    cancellationTokenSource.Cancel(false);
                }
                return Task.CompletedTask;
            }, cancellationTokenSource.Token);

            var writeDataToStreamTask = WriteChunckedMessagesToStream(msg, expectedMessageCount, stream, cancellationTokenSource);

            try
            {
                await Task.WhenAll(readDataFromStreamTask, readMessageTask, writeDataToStreamTask);
            }
            catch(TaskCanceledException ex)
            {

            }

            //assert
            Assert.AreEqual(expectedMessageCount, receivedMessageCount);
        }

        [Test]
        public async Task ReadMessages()
        {
            //arrange
            var msg =
                "8=FIXT.1.19=41935=634=675049=MA52=20200210-15:25:06.69056=280AXEOT1128=923=1054893731-0+128=R26=1054893731-0+055=[N/A]48=79642BXT522=1454=1455=US79642BXT50456=4167=NONE762=MUNI54=227=20000015=USD232=1233=MINQTY234=5000423=144=112.796640=112.7960=20200210-15:24:59236=1.0453=1448=XrefID-33447=D452=75961=MNPrice22022=120,300,900,180020120=ClientAxe-Orders20014=0.05620015=0.02829735=1054893731-0+010=019";

            var receivedMessageCount = 0;
            var expectedMessageCount = 10000;
            var server = new Server(9212);
            var serverSideTcpClientTask = server.WaitForClient(CancellationToken.None);
            await Task.Delay(2000);
            var client = new Client();
            using var cancellationTokenSource = new CancellationTokenSource();
            using var clientStream = client.Connect(9212);
            using var serverStream = (await serverSideTcpClientTask).GetStream();

            //act
            var readDataFromStreamTask = _target.ReadStreamData(clientStream, cancellationTokenSource.Token);
            var readMessageTask = _target.ReadMessages((m, ct) =>
            {
                receivedMessageCount++;
                if (receivedMessageCount == expectedMessageCount)
                {
                    cancellationTokenSource.Cancel();
                    clientStream.Dispose();
                    serverStream.Dispose();
                }
                return Task.CompletedTask;
            }, cancellationTokenSource.Token);

            var writeDataToStreamTask = WriteChunckedMessagesToStream(msg, expectedMessageCount, serverStream, cancellationTokenSource);

            try
            {
                await Task.WhenAll(readDataFromStreamTask, readMessageTask, writeDataToStreamTask);
            }
            catch (ObjectDisposedException ex)
            {
                //closed stream
            }
            catch (TaskCanceledException ex)
            {

            }

            //assert
            Assert.AreEqual(expectedMessageCount, receivedMessageCount);
        }

        private static Task WriteChunckedMessagesToStream(string msg, int count, Stream stream, CancellationTokenSource cancellationTokenSource)
        {
            var chunckedMessage = Enumerable.Range(1, msg.Length / 2).Select(i => new string(msg.Skip(i * 2).Take(2).ToArray())).ToList();
            return Task.Run(() =>
            {
                using var streamWriter = new StreamWriter(stream, CharEncoding.DefaultEncoding, 1024, true);
                for (var i = 0; i < count; i++)
                    chunckedMessage.ForEach(chunk =>
                    {
                        streamWriter.Write(chunk);
                        streamWriter.Flush();
                    });
            }, cancellationTokenSource.Token);
        }


        private class Server : IDisposable
        {
            private readonly TcpListener _listener;
            public Server(int port)
            {
                _listener = new TcpListener(new IPEndPoint(IPAddress.Any, port));
            }
            public Task<TcpClient> WaitForClient(CancellationToken cancellationToken)
            {
                return Task.Run(() =>
                {
                    _listener.Start();
                    return _listener.AcceptTcpClient();
                }, cancellationToken);
            }

            public void Dispose()
            {
                _listener.Server.Dispose();
            }
        }

        private class Client
        {
            public Stream Connect(int port)
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = true,
                    LingerState = { Enabled = false }
                };
                socket.Connect(IPAddress.Parse("127.0.0.1"), port);
                return new NetworkStream(socket, true);
            }
        }

        private class ProducerConsumerStream : Stream
        {
            private readonly MemoryStream innerStream;
            private long readPosition;
            private long writePosition;

            public ProducerConsumerStream()
            {
                innerStream = new MemoryStream();
            }

            public override bool CanRead => true;

            public override bool CanSeek => false;

            public override bool CanWrite => true;

            public override void Flush()
            {
                lock (innerStream)
                {
                    innerStream.Flush();
                }
            }

            public override long Length
            {
                get
                {
                    lock (innerStream)
                    {
                        return innerStream.Length;
                    }
                }
            }

            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                lock (innerStream)
                {
                    innerStream.Position = readPosition;
                    int red = innerStream.Read(buffer, offset, count);
                    readPosition = innerStream.Position;

                    return red;
                }
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                lock (innerStream)
                {
                    innerStream.Position = writePosition;
                    innerStream.Write(buffer, offset, count);
                    writePosition = innerStream.Position;
                }
            }
        }
    }
}
