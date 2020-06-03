using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.IO;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace QuickFix
{
    /// <summary>
    /// Handles a connection with an acceptor.
    /// </summary>
    public class SocketInitiatorThread : IResponder
    {
        public Session.Session Session { get { return session_; } }
        public Transport.SocketInitiator Initiator { get { return initiator_; } }

        public const int BUF_SIZE = 512;

        //private Thread thread_ = null;
        //private Task _task;
        private byte[] readBuffer_ = new byte[BUF_SIZE];
        private MessageReader _messageReader;
        protected Stream stream_;
        private Transport.SocketInitiator initiator_;
        private Session.Session session_;
        private IPEndPoint socketEndPoint_;
        protected SocketSettings socketSettings_;
        private bool isDisconnectRequested_ = false;

        public SocketInitiatorThread(Transport.SocketInitiator initiator, Session.Session session, IPEndPoint socketEndPoint, SocketSettings socketSettings)
        {
            isDisconnectRequested_ = false;
            initiator_ = initiator;
            session_ = session;
            socketEndPoint_ = socketEndPoint;
            _messageReader = new MessageReader();
            session_ = session;
            socketSettings_ = socketSettings;
        }

        public Task Start(CancellationToken cancellationToken)
        {
            isDisconnectRequested_ = false;
            return Transport.SocketInitiator.SocketInitiatorThreadStart(this, cancellationToken);
        }

        public Task Connect(CancellationToken cancellationToken)
        {
            Debug.Assert(stream_ == null);

            stream_ = SetupStream();
            return session_.SetResponder(this, cancellationToken);
        }

        /// <summary>
        /// Setup/Connect to the other party.
        /// Override this in order to setup other types of streams with other settings
        /// </summary>
        /// <returns>Stream representing the (network)connection to the other party</returns>
        protected virtual Stream SetupStream()
        {
            return QuickFix.Transport.StreamFactory.CreateClientStream(socketEndPoint_, socketSettings_, session_.Log);
        }

        public async Task HandleSessionLifeCycle(CancellationToken cancellationToken)
        {
            while (cancellationToken.IsCancellationRequested)
            {
                if(session_ != default) await session_.Next(cancellationToken);
                //TODO: should be replaced by timer it will be much efficient
                await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken);
            }
            cancellationToken.ThrowIfCancellationRequested();
        }

        public Task ParseMessages(CancellationToken cancellationToken)
        {
            return _messageReader.ReadMessages((msg, ct) => session_?.Next(msg, ct), cancellationToken);
        }

        public async Task ReadData(CancellationToken cancellationToken)
        {
            try
            {
                await _messageReader.ReadStreamData(this.stream_, cancellationToken).ConfigureAwait(false);
            }
            catch (ObjectDisposedException e)
            {
                // this exception means socket_ is already closed when poll() is called
                if (isDisconnectRequested_ == false)
                {
                    // for lack of a better idea, do what the general exception does
                    if (null != session_)
                        await session_.Disconnect(e.ToString(), cancellationToken);
                    else
                        Disconnect();
                }
            }
            catch (Exception e)
            {
                if (null != session_)
                    await session_.Disconnect(e.ToString(), cancellationToken);
                else
                    Disconnect();
            }
        }

        #region Responder Members

        public bool Send(string data)
        {
            var rawData = CharEncoding.DefaultEncoding.GetBytes(data);
            stream_.Write(rawData, 0, rawData.Length);
            return true;
        }

        public void Disconnect()
        {
            isDisconnectRequested_ = true;
            stream_?.Close();
        }

        #endregion
    }
}
