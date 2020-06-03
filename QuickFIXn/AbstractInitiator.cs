using System.Collections.Concurrent;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QuickFix
{
    public abstract class AbstractInitiator : IInitiator
    {
        // from constructor
        private IApplication _app = null;
        private IMessageStoreFactory _storeFactory = null;
        private SessionSettings _settings = null;
        private ILogFactory _logFactory = null;
        private IMessageFactory _msgFactory = null;

        private object sync_ = new object();
        private bool _disposed = false;
        private readonly ConcurrentDictionary<SessionID, Session.Session> _sessions = new ConcurrentDictionary<SessionID, Session.Session>();
        private bool isStopped_ = true;
        private readonly SessionFactory _sessionFactory = null;

        #region Properties

        public bool IsStopped => isStopped_;

        #endregion
        //#endregion

        protected AbstractInitiator(
            IApplication app, IMessageStoreFactory storeFactory, SessionSettings settings, ILogFactory logFactory = default, IMessageFactory messageFactory = default)
        {
            _app = app;
            _storeFactory = storeFactory;
            _settings = settings;
            _logFactory = logFactory ?? new NullLogFactory();
            _msgFactory = messageFactory ?? new DefaultMessageFactory();
            _sessionFactory = new SessionFactory(_app, _storeFactory, _logFactory, _msgFactory);

            var definedSessions = _settings.GetSessions();
            if (!definedSessions.Any())
                throw new ConfigError("No sessions defined");
        }

        public Task Start(CancellationToken cancellationToken)
        {
            if (_disposed)
                throw new System.ObjectDisposedException(GetType().Name);

            // create all sessions
            foreach (var sessionId in _settings.GetSessions())
            {
                var dict = _settings.Get(sessionId);
                CreateSession(sessionId, dict, cancellationToken);
            }

            if (_sessions.IsEmpty)
                throw new ConfigError("No sessions defined for initiator");

            // start it up
            isStopped_ = false;
            OnConfigure(_settings);
            return OnStart(cancellationToken);
        }

        /// <summary>
        /// Add new session as an ad-hoc (dynamic) operation
        /// </summary>
        /// <param name="sessionId">ID of new session</param>
        /// <param name="dict">config settings for new session</param>
        /// <param name="cancellationToken">Root cancellation token that use to shutdown fix app</param>
        /// <returns>true if session added successfully, false if session already exists or is not an initiator</returns>
        public bool AddSession(SessionID sessionId, Dictionary dict, CancellationToken cancellationToken)
        {
            if (!_settings.TrySet(sessionId, dict)) return false; // session won't be in settings if ad-hoc creation after startup
            if (CreateSession(sessionId, dict, cancellationToken)) return true;
            if(!_settings.Remove(sessionId)) throw new ConfigError("Session can't be removed at runtime");
            return false;
        }

        /// <summary>
        /// Create session, either at start-up or as an ad-hoc operation
        /// </summary>
        /// <param name="sessionId">ID of new session</param>
        /// <param name="dict">config settings for new session</param>
        /// <param name="cancellationToken">Root cancellation token that use to shutdown fix app</param>
        /// <returns>true if session added successfully, false if session already exists or is not an initiator</returns>
        private bool CreateSession(SessionID sessionId, Dictionary dict, CancellationToken cancellationToken)
        {
            if (dict.GetString(SessionSettings.CONNECTION_TYPE) != "initiator" || _sessions.TryGetValue(sessionId, out _)) 
                return false;
            
            var session = _sessionFactory.Create(sessionId, dict, cancellationToken);
            session.ConnectionState.SetDisconnected();
            return _sessions.TryAdd(sessionId, session);
        }

        /// <summary>
        /// Ad-hoc removal of an existing session
        /// </summary>
        /// <param name="sessionID">ID of session to be removed</param>
        /// <param name="terminateActiveSession">if true, force disconnection and removal of session even if it has an active connection</param>
        /// <param name="cancellationToken"></param>
        /// <returns>true if session removed or not already present; false if could not be removed due to an active connection</returns>
        public async Task<bool> RemoveSession(SessionID sessionID, bool terminateActiveSession, CancellationToken cancellationToken)
        {
            if (!_sessions.TryGetValue(sessionID, out var session)) return false;
            using (await session.CriticalSection.EnterAsync(cancellationToken))
            {
                var sessionDetails = await session.GetDetails(cancellationToken);
                if (sessionDetails.IsLoggedOn && !terminateActiveSession) return false;
                if (!_sessions.TryRemove(sessionID, out session)) return false;
                _settings.Remove(sessionID);
                if(session.ConnectionState.CanDisconnect)
                    session.Disconnect("Dynamic session removal");
                OnRemove(sessionID); // ensure session's reader thread is gone before we dispose session
            }
            session.Dispose();
            return true;
        }

        /// <summary>
        /// Logout existing session and close connection.  Attempt graceful disconnect first.
        /// </summary>
        public Task Stop(CancellationToken cancellationToken) => Stop(false, cancellationToken);

        /// <summary>
        /// Logout existing session and close connection
        /// </summary>
        /// <param name="force">If true, terminate immediately.  </param>
        /// <param name="cancellationToken"></param>
        public async Task Stop(bool force, CancellationToken cancellationToken)
        {
            if (_disposed)
                throw new System.ObjectDisposedException(this.GetType().Name);

            if (IsStopped)
                return;

            var connectedSessions = _sessions.Values.Where(s => s.ConnectionState.IsConnected).ToArray();

            foreach (var session in connectedSessions)
            {
                var sessionDetails = await session.GetDetails(cancellationToken);
                if (!sessionDetails.IsEnabled) continue;
                session.Logout();
            }

            if (!force)
            {
                // TODO change this duration to always exceed LogoutTimeout setting
                for (int second = 0; (second < 10) && (await IsLoggedOn(cancellationToken)); ++second)
                    Thread.Sleep(1000);
            }

            _sessions.Values
                .Where(s => s.ConnectionState.IsConnected)
                .ToList()
                .ForEach(s => s.ConnectionState.SetDisconnected());
            isStopped_ = true;
            OnStop();

            // Give OnStop() time to finish its business

            // dispose all sessions and clear all session sets
           foreach (var s in _sessions.Values)
                s.Dispose();
           _sessions.Clear();
        }

        //TODO: nmandzyk - should analyzed more precisely 
        public async Task<bool> IsLoggedOn(CancellationToken cancellationToken)
        {

            foreach (var session in _sessions.Values)
            {
                if (session.ConnectionState.IsConnected &&
                    (await session.GetDetails(cancellationToken)).IsLoggedOn) return true;
            }

            return false;
        }

        #region Virtual Methods

        /// <summary>
        /// Override this to configure additional implemenation-specific settings
        /// </summary>
        /// <param name="settings"></param>
        protected virtual void OnConfigure(SessionSettings settings)
        { }

        /// <summary>
        /// Implement this to provide custom reaction behavior to an ad-hoc session removal.
        /// (This is called after the session is removed.)
        /// </summary>
        /// <param name="sessionID">ID of session that was removed</param>
        protected virtual void OnRemove(SessionID sessionID)
        { }

        [System.Obsolete("This method's intended purpose is unclear.  Don't use it.")]
        protected virtual void OnInitialize(SessionSettings settings)
        { }

        #endregion

        #region Abstract Methods

        /// <summary>
        /// Implemented to start connecting to targets.
        /// </summary>
        protected abstract Task OnStart(CancellationToken cancellationToken);

        /// <summary>
        /// Implemented to stop a running initiator.
        /// </summary>
        protected abstract void OnStop();

        /// <summary>
        /// Implemented to connect a session to its target.
        /// </summary>
        /// <param name="session"></param>
        /// <param name="settings"></param>
        /// <param name="cancellationToken"></param>
        protected abstract Task DoConnect(Session.Session session, QuickFix.Dictionary settings, CancellationToken cancellationToken);

        #endregion

        #region Protected Methods

        protected async Task Connect(CancellationToken cancellationToken)
        {
            var sessions =  _sessions.Values.Where(s => s.ConnectionState.IsDisconnected).ToArray();
            foreach (var session in sessions)
            {
                // TODO: nmandzyk should be revised why we store all session in Session object
                //Session session = Session.LookupSession(sessionID);
                var sessionDetails = await session.GetDetails(cancellationToken);
                if (!sessionDetails.IsEnabled) continue;

                if (session.IsNewSession)
                    await session.Reset("New session", cancellationToken);
                if (session.IsSessionTime)
                    await DoConnect(session, _settings.Get(session.SessionID), cancellationToken);
            }
        }
        #endregion


        /// <summary>
        /// Get the SessionIDs for the sessions managed by this initiator.
        /// </summary>
        /// <returns>the SessionIDs for the sessions managed by this initiator</returns>
        public HashSet<SessionID> GetSessionIDs() => new HashSet<SessionID>(_sessions.Keys);

        /// <summary>
        /// Any subclasses of AbstractInitiator should override this if they have resources to dispose
        /// that aren't already covered in its OnStop() handler.
        /// Any override should call base.Dispose(disposing).
        /// </summary>
        /// <param name="disposing"></param>
        /// <param name="cancellationToken"></param>
        protected virtual async Task Dispose(bool disposing, CancellationToken cancellationToken)
        {
            await Stop(cancellationToken);
            _disposed = true;
        }

        public void Dispose()
        {
            Dispose(true, CancellationToken.None).GetAwaiter().GetResult();
        }
    }
}
