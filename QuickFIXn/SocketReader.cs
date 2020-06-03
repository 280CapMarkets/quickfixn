using System.Net.Sockets;
using System.IO;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using QuickFix.Session;

namespace QuickFix
{

    /// <summary>
    /// TODO merge with SocketInitiatorThread
    /// </summary>
    public class SocketReader : IDisposable
    {
        public const int BUF_SIZE = 4096;
        byte[] readBuffer_ = new byte[BUF_SIZE];
        private Parser parser_ = new Parser();
        private Session.Session qfSession_; //will be null when initialized
        private Stream stream_;     //will be null when initialized
        private TcpClient tcpClient_;
        private ClientHandlerThread responder_;
        private readonly AcceptorSocketDescriptor acceptorDescriptor_;

        /// <summary>
        /// Keep a handle to the current outstanding read request (if any)
        /// </summary>
        private IAsyncResult currentReadRequest_;

        [Obsolete("Use other constructor")]
        public SocketReader(TcpClient tcpClient, ClientHandlerThread responder)
            : this(tcpClient, new SocketSettings(), responder)
        {
        }

        public SocketReader(TcpClient tcpClient, SocketSettings settings, ClientHandlerThread responder)
            : this(tcpClient, settings, responder, null)
        {
            
        }

        internal SocketReader(
            TcpClient tcpClient,
            SocketSettings settings,
            ClientHandlerThread responder,
            AcceptorSocketDescriptor acceptorDescriptor)
        {
            tcpClient_ = tcpClient;
            responder_ = responder;
            acceptorDescriptor_ = acceptorDescriptor;
            stream_ = Transport.StreamFactory.CreateServerStream(tcpClient, settings, responder.GetLog());
        }

        /// <summary> FIXME </summary>
        public async Task Read(CancellationToken cancellationToken)
        {
            try
            {
                var bytesRead = ReadSome(readBuffer_, 1000);
                if (bytesRead > 0)
                    parser_.AddToStream(readBuffer_, bytesRead);
                else if (null != qfSession_ && qfSession_.Disposed == false
                ) //TODO: nmandzyk strange why session is not null when it was disposed (should be verified when sever will fix)
                {
                    await qfSession_.Next(cancellationToken);
                }

                await ProcessStream(cancellationToken);
            }
            catch (MessageParseError e)
            {
                await HandleExceptionInternal(qfSession_, e, cancellationToken);
            }
            catch (Exception e)
            {
                await HandleExceptionInternal(qfSession_, e, cancellationToken);
                throw e;
            }
        }

        /// <summary>
        /// Reads data from the network into the specified buffer.
        /// It will wait up to the specified number of milliseconds for data to arrive,
        /// if no data has arrived after the specified number of milliseconds then the function returns 0
        /// </summary>
        /// <param name="buffer">The buffer.</param>
        /// <param name="timeoutMilliseconds">The timeout milliseconds.</param>
        /// <returns>The number of bytes read into the buffer</returns>
        /// <exception cref="System.Net.Sockets.SocketException">On connection reset</exception>
        protected virtual int ReadSome(byte[] buffer, int timeoutMilliseconds)
        {
            // NOTE: THIS FUNCTION IS EXACTLY THE SAME AS THE ONE IN SocketInitiatorThread.
            // Any changes made here should also be made there.
            try
            {
                if (!stream_.CanRead)
                    return 0;
                // Begin read if it is not already started
                if (currentReadRequest_ == null)
                    currentReadRequest_ = stream_.BeginRead(buffer, 0, buffer.Length, null, null);

                // Wait for it to complete (given timeout)
                currentReadRequest_.AsyncWaitHandle.WaitOne(timeoutMilliseconds);

                if (currentReadRequest_.IsCompleted)
                {
                    // Make sure to set currentReadRequest_ to before retreiving result 
                    // so a new read can be started next time even if an exception is thrown
                    var request = currentReadRequest_;
                    currentReadRequest_ = null;

                    int bytesRead = stream_.EndRead(request);
                    if (0 == bytesRead)
                        throw new SocketException(System.Convert.ToInt32(SocketError.ConnectionReset));

                    return bytesRead;
                }
                else
                    return 0;
            }
            catch (System.IO.IOException ex) // Timeout
            {
                var inner = ex.InnerException as SocketException;
                if (inner != null && inner.SocketErrorCode == SocketError.TimedOut)
                {
                    // Nothing read 
                    return 0;
                }
                else if (inner != null)
                {
                    throw inner; //rethrow SocketException part (which we have exception logic for)
                }
                else
                    throw; //rethrow original exception
            }
        }

        [Obsolete("This should be made private")]
        public void OnMessageFound(string msg)
        {
            OnMessageFoundInternal(msg, CancellationToken.None).GetAwaiter().GetResult();
        }

        protected async Task OnMessageFoundInternal(string msg, CancellationToken cancellationToken)
        {
            try
            {
                if (null == qfSession_)
                {
                    qfSession_ = Session.Session.LookupSession(Message.GetReverseSessionID(msg));
                    if (null == qfSession_)
                    {
                        this.Log("ERROR: Disconnecting; received message for unknown session: " + msg);
                        DisconnectClient();
                        return;
                    }
                    else if(IsAssumedSession(qfSession_.SessionID))
                    {
                        this.Log("ERROR: Disconnecting; received message for unknown session: " + msg);
                        qfSession_ = null;
                        DisconnectClient();
                        return;
                    }
                    else
                    {
                        if (!(await HandleNewSession(msg, cancellationToken)))
                            return;
                    }
                }

                try
                {
                    await qfSession_.Next(msg, cancellationToken);
                }
                catch (System.Exception e)
                {
                    this.Log("Error on Session '" + qfSession_.SessionID + "': " + e.ToString());
                }
            }
            catch (InvalidMessage e)
            {
                HandleBadMessage(msg, e);
            }
            catch (MessageParseError e)
            {
                HandleBadMessage(msg, e);
            }
        }

        protected void HandleBadMessage(string msg, System.Exception e)
        {
            try
            {
                if (Fields.MsgType.LOGON.Equals(Message.GetMsgType(msg)))
                {
                    this.Log("ERROR: Invalid LOGON message, disconnecting: " + e.Message);
                    DisconnectClient();
                }
                else
                {
                    this.Log("ERROR: Invalid message: " + e.Message);
                }
            }
            catch (InvalidMessage)
            { }
        }

        protected bool ReadMessage(out string msg)
        {
            try
            {
                return parser_.ReadFixMessage(out msg);
            }
            catch (MessageParseError e)
            {
                msg = "";
                throw e;
            }
        }

        protected async Task ProcessStream(CancellationToken cancellationToken)
        {
            while (ReadMessage(out var msg))
                await OnMessageFoundInternal(msg, cancellationToken);
        }

        [Obsolete("Static function can't close stream properly")]
        protected static void DisconnectClient(TcpClient client)
        {
            client.Client.Close();
            client.Close();
        }

        protected void DisconnectClient()
        {
            stream_.Close();
            tcpClient_.Close();
        }

        protected async Task<bool> HandleNewSession(string msg, CancellationToken cancellationToken)
        {
            //TODO: nmandzyk should revised 
            var sessionDetails = await qfSession_.GetDetails(cancellationToken);
            if (sessionDetails.HasResponder)
            {
                qfSession_.Log.OnIncoming(msg);
                qfSession_.Log.OnEvent("Multiple logons/connections for this session are not allowed (" + tcpClient_.Client.RemoteEndPoint + ")");
                qfSession_ = null;
                DisconnectClient();
                return false;
            }
            qfSession_.Log.OnEvent(qfSession_.SessionID + " Socket Reader " + GetHashCode() + " accepting session " + qfSession_.SessionID + " from " + tcpClient_.Client.RemoteEndPoint);
            // FIXME do this here? qfSession_.HeartBtInt = QuickFix.Fields.Converters.IntConverter.Convert(message.GetField(Fields.Tags.HeartBtInt)); /// FIXME
            sessionDetails = await qfSession_.GetDetails(cancellationToken);
            qfSession_.Log.OnEvent(qfSession_.SessionID + " Acceptor heartbeat set to " + sessionDetails.HeartBtInt + " seconds");
            await qfSession_.SetResponder(responder_, cancellationToken);
            return true;
        }

        [Obsolete("This should be made private/protected")]
        public Task HandleException(Session.Session quickFixSession, System.Exception cause, TcpClient client, CancellationToken cancellationToken) => HandleExceptionInternal(quickFixSession, cause, cancellationToken);

        private bool IsAssumedSession(SessionID sessionID)
        {
            return acceptorDescriptor_ != null 
                   && !acceptorDescriptor_.GetAcceptedSessions().Any(kv => kv.Key.Equals(sessionID));
        }

        private async Task HandleExceptionInternal(Session.Session quickFixSession, System.Exception cause, CancellationToken cancellationToken)
        {
            bool disconnectNeeded = true;
            string reason = cause.Message;

            System.Exception realCause = cause;

            // Unwrap socket exceptions from IOException in order for code below to work
            if (realCause is IOException && realCause.InnerException is SocketException)
                realCause = realCause.InnerException;

            /*
             TODO
            if(cause is FIXMessageDecoder.DecodeError && cause.InnerException != null)
                realCause = cause.getCause();
            */
            if (realCause is System.Net.Sockets.SocketException)
            {
                var sessionDetails = quickFixSession != default ? await quickFixSession.GetDetails(cancellationToken) : default;
                if (sessionDetails != null && sessionDetails.IsEnabled)
                    reason = "Socket exception (" + tcpClient_.Client.RemoteEndPoint + "): " + cause.Message;
                else
                    reason = "Socket (" + tcpClient_.Client.RemoteEndPoint + "): " + cause.Message;
                disconnectNeeded = true;
            }
            /*
             TODO
            else if(realCause is FIXMessageDecoder.CriticalDecodeError)
            {
                reason = "Critical protocol codec error: " + cause;
                disconnectNeeded = true;
            }
            */
            else if (realCause is MessageParseError)
            {
                reason = "Protocol handler exception: " + cause;
                if (quickFixSession == null)
                    disconnectNeeded = true;
                else
                    disconnectNeeded = false;
            }
            else
            {
                reason = cause.ToString();
                disconnectNeeded = false;
            }

            this.Log("SocketReader Error: " + reason);

            if (disconnectNeeded)
            {
                if (null != quickFixSession && (await qfSession_.GetDetails(cancellationToken)).HasResponder)
                    await quickFixSession.Disconnect(reason, cancellationToken);
                else
                    DisconnectClient();
            }
        }

        /// <summary>
        /// FIXME do proper logging
        /// </summary>
        /// <param name="s"></param>
        private void Log(string s)
        {
            responder_.Log(s);
        }

        public int Send(string data)
        {
            byte[] rawData = CharEncoding.DefaultEncoding.GetBytes(data);
            stream_.Write(rawData, 0, rawData.Length);
            return rawData.Length;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                stream_.Dispose();
                tcpClient_.Close();
            }
        }
    }
}
