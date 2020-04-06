using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using QuickFix.Config;
using System.Net;
using System.Diagnostics;
using System.Threading;
using System.IO;
using System.Threading.Tasks;

namespace QuickFix.Transport
{
    /// <summary>
    /// Initiates connections and uses a single thread to process messages for all sessions.
    /// </summary>
    public class SocketInitiator : AbstractInitiator
    {
        public const string SOCKET_CONNECT_HOST = "SocketConnectHost";
        public const string SOCKET_CONNECT_PORT = "SocketConnectPort";
        public const string RECONNECT_INTERVAL  = "ReconnectInterval";

        #region Private Members

        private volatile bool shutdownRequested_ = false;
        private DateTime lastConnectTimeDT = DateTime.MinValue;
        private int reconnectInterval_ = 30;
        private SocketSettings socketSettings_ = new SocketSettings();
        private ConcurrentDictionary<SessionID, Task> _workerTasks = new ConcurrentDictionary<SessionID, Task>();
        private Dictionary<SessionID, int> sessionToHostNum_ = new Dictionary<SessionID, int>();
        
        #endregion

        public SocketInitiator(IApplication application, IMessageStoreFactory storeFactory, SessionSettings settings, ILogFactory logFactory = default, IMessageFactory messageFactory = default)
            : base(application, storeFactory, settings, logFactory, messageFactory)
        { }

        public static async Task SocketInitiatorThreadStart(SocketInitiatorThread socketInitiatorThread)
        {
            if (socketInitiatorThread == null) return;
            string exceptionEvent = null;
            try
            {
                try
                {
                    var session = socketInitiatorThread.Session;
                    socketInitiatorThread.Connect();
                    socketInitiatorThread.Session.ConnectionState.SetConnected();
                    session.Log.OnEvent("Connection succeeded");
                    session.Next();

                    var sessionTask = socketInitiatorThread.HandleSessionLifeCycle(session.SessionCancellationToken);
                    var readTask = socketInitiatorThread.ReadData(session.SessionCancellationToken);
                    var parsingTask = socketInitiatorThread.ParseMessages(session.SessionCancellationToken);

                    await Task.WhenAll(sessionTask, readTask, parsingTask);

                    
                    if (socketInitiatorThread.Initiator.IsStopped)
                        await socketInitiatorThread.Initiator.RemoveThread(socketInitiatorThread);
                    session.ConnectionState.SetDisconnected();
                }
                catch (IOException ex) // Can be exception when connecting, during ssl authentication or when reading
                {
                    exceptionEvent = $"Connection failed: {ex.Message}";
                }
                catch (SocketException e)
                {
                    exceptionEvent = $"Connection failed: {e.Message}";
                }
                catch (System.Security.Authentication.AuthenticationException ex) // some certificate problems
                {
                    exceptionEvent = $"Connection failed (AuthenticationException): {ex.Message}";
                }
                catch (Exception ex)
                {
                    exceptionEvent = $"Unexpected exception: {ex}";
                }

                if (exceptionEvent != null)
                {
                    if (socketInitiatorThread.Session.Disposed)
                    {
                        // The session is disposed, and so is its log. We cannot use it to log the event,
                        // so we resort to storing it in a local file.
                        try
                        {
                            File.AppendAllText("DisposedSessionEvents.log", $"{System.DateTime.Now:G}: {exceptionEvent}{Environment.NewLine}");
                        }
                        catch (IOException)
                        {
                            // Prevent IO exceptions from crashing the application
                        }
                    }
                    else
                    {
                        socketInitiatorThread.Session.Log.OnEvent(exceptionEvent);
                    }
                }
            }
            finally
            {
                await socketInitiatorThread.Initiator.RemoveThread(socketInitiatorThread);
                socketInitiatorThread.Session.ConnectionState.SetDisconnected();
            }
        }

        private async Task RemoveThread(SocketInitiatorThread thread)
        {
            using (thread.Session.CriticalSection.EnterAsync())
            {
                if (_workerTasks.TryGetValue(thread.Session.SessionID, out var task))
                {
                    await task;
                    _workerTasks.TryRemove(thread.Session.SessionID, out _);
                }
            }
        }       

        private IPEndPoint GetNextSocketEndPoint(Session session, QuickFix.Dictionary settings)
        {
            if (!sessionToHostNum_.TryGetValue(session.SessionID, out var num))
                num = 0;

            var hostKey = SessionSettings.SOCKET_CONNECT_HOST + num;
            var portKey = SessionSettings.SOCKET_CONNECT_PORT + num;
            if (!settings.Has(hostKey) || !settings.Has(portKey))
            {
                num = 0;
                hostKey = SessionSettings.SOCKET_CONNECT_HOST;
                portKey = SessionSettings.SOCKET_CONNECT_PORT;
            }

            try
            {
                var hostName = settings.GetString(hostKey);
                var addrs = Dns.GetHostAddresses(hostName);
                var port = settings.GetInt(portKey);
                sessionToHostNum_[session.SessionID] = ++num;

                socketSettings_.ServerCommonName = hostName;
                return new IPEndPoint(addrs.First(a => a.AddressFamily == AddressFamily.InterNetwork), port);
            }
            catch (System.Exception e)
            {
                throw new ConfigError(e.Message, e);
            }
        }

        #region Initiator Methods
        
        /// <summary>
        /// handle other socket options like TCP_NO_DELAY here
        /// </summary>
        /// <param name="settings"></param>
        protected override void OnConfigure(SessionSettings settings)
        {
            try
            {
                reconnectInterval_ = settings.GetDefaultSettings().GetInt(SessionSettings.RECONNECT_INTERVAL);
            }
            catch (System.Exception)
            { }

            // Don't know if this is required in order to handle settings in the general section
            socketSettings_.Configure(settings.GetDefaultSettings());
        }       

        protected override async Task OnStart(CancellationToken cancellationToken)
        {
            var span = TimeSpan.FromSeconds(reconnectInterval_);
            var loopTimeout = TimeSpan.FromSeconds(1);
            while(!cancellationToken.IsCancellationRequested)
            {
                var utcDateTimeNow = DateTime.UtcNow;
                if ((utcDateTimeNow.Subtract(lastConnectTimeDT).TotalMilliseconds) >= span.TotalMilliseconds)
                {
                    Connect();
                    lastConnectTimeDT = utcDateTimeNow;
                }
                await Task.Delay(loopTimeout, cancellationToken);
            }
        }

        /// <summary>
        /// Ad-hoc session removal
        /// </summary>
        /// <param name="sessionID">ID of session being removed</param>
        protected override void OnRemove(SessionID sessionID)
        {
            _workerTasks.TryRemove(sessionID, out _);
        }

        protected override void OnStop()
        {
            shutdownRequested_ = true;
        }

        protected override void DoConnect(Session session, Dictionary settings)
        {
            try
            {
                //session = Session.LookupSession(sessionID);
                if (!session.IsSessionTime) return;

                var socketEndPoint = GetNextSocketEndPoint(session, settings);
                session.ConnectionState.SetPending();
                session.Log.OnEvent("Connecting to " + socketEndPoint.Address + " on port " + socketEndPoint.Port);

                //Setup socket settings based on current section
                var socketSettings = socketSettings_.Clone();
                socketSettings.Configure(settings);

                // Create a Ssl-SocketInitiatorThread if a certificate is given
                var t = new SocketInitiatorThread(this, session, socketEndPoint, socketSettings);                
                var workerTask =  t.Start();
                if (!_workerTasks.TryAdd(session.SessionID, workerTask))
                {
                    throw new InvalidOperationException("Socket initiator thread already exists");
                }
            }
            catch (System.Exception e)
            {
                session?.Log.OnEvent(e.Message);
            }
        }

        #endregion
    }
}
