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
        public Session Session { get { return session_; } }
        public Transport.SocketInitiator Initiator { get { return initiator_; } }

        public const int BUF_SIZE = 512;

        private Thread thread_ = null;
        private byte[] readBuffer_ = new byte[BUF_SIZE];
        private MessageReader _messageReader;
        protected Stream stream_;
        private Transport.SocketInitiator initiator_;
        private Session session_;
        private IPEndPoint socketEndPoint_;
        protected SocketSettings socketSettings_;
        private bool isDisconnectRequested_ = false;

        public SocketInitiatorThread(Transport.SocketInitiator initiator, Session session, IPEndPoint socketEndPoint, SocketSettings socketSettings)
        {
            isDisconnectRequested_ = false;
            initiator_ = initiator;
            session_ = session;
            socketEndPoint_ = socketEndPoint;
            _messageReader = new MessageReader();
            session_ = session;
            socketSettings_ = socketSettings;
        }

        public void Start()
        {
            isDisconnectRequested_ = false;
            thread_ = new Thread(new ParameterizedThreadStart(Transport.SocketInitiator.SocketInitiatorThreadStart));
            thread_.Start(this);
        }

        public void Join()
        {
            if (null == thread_)
                return;
            Disconnect();
            // Make sure session's socket reader thread doesn't try to do a Join on itself!
            if (Thread.CurrentThread.ManagedThreadId != thread_.ManagedThreadId)
                thread_.Join(2000);
            thread_ = null;
        }

        public void Connect()
        {
            Debug.Assert(stream_ == null);

            stream_ = SetupStream();
            session_.SetResponder(this);
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
                session_?.Next();
                await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken);
            }
        }

        public Task ParseMessages(CancellationToken cancellationToken)
        {
            return _messageReader.ReadMessages((msg) =>
            {
                session_?.Next(msg);
            }, cancellationToken);
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
                        session_.Disconnect(e.ToString());
                    else
                        Disconnect();
                }
            }
            catch (Exception e)
            {
                if (null != session_)
                    session_.Disconnect(e.ToString());
                else
                    Disconnect();
            }
        }

        #region Responder Members

        public bool Send(string data)
        {
            byte[] rawData = CharEncoding.DefaultEncoding.GetBytes(data);
            stream_.Write(rawData, 0, rawData.Length);
            return true;
        }

        public void Disconnect()
        {
            isDisconnectRequested_ = true;
            if (stream_ != null)
                stream_.Close();
        }

        #endregion
    }
}
