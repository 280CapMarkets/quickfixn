using System.Collections.Generic;
using System.Net;

namespace QuickFix
{
    internal class AcceptorSocketDescriptor
    {
        #region Properties

        public ThreadedSocketReactor SocketReactor
        {
            get { return socketReactor_; }
        }

        public IPEndPoint Address
        {
            get { return socketEndPoint_; }
        }

        #endregion

        #region Private Members

        private ThreadedSocketReactor socketReactor_;
        private IPEndPoint socketEndPoint_;
        private Dictionary<SessionID, Session.Session> acceptedSessions_ = new Dictionary<SessionID, Session.Session>();

        #endregion

        public AcceptorSocketDescriptor(IPEndPoint socketEndPoint, SocketSettings socketSettings, QuickFix.Dictionary sessionDict)
        {
            socketEndPoint_ = socketEndPoint;
            socketReactor_ = new ThreadedSocketReactor(socketEndPoint_, socketSettings, sessionDict, this);
        }

        public void AcceptSession(Session.Session session)
        {
            lock (acceptedSessions_)
            {
                acceptedSessions_[session.SessionID] = session;
            }
        }

        /// <summary>
        /// Remove a session from those tied to this socket.
        /// </summary>
        /// <param name="sessionID">ID of session to be removed</param>
        /// <returns>true if session removed, false if not found</returns>
        public bool RemoveSession(SessionID sessionID)
        {
            lock (acceptedSessions_)
            {
                return acceptedSessions_.Remove(sessionID);
            }
        }

        public Dictionary<SessionID, Session.Session> GetAcceptedSessions()
        {
            lock (acceptedSessions_)
            {
                return new Dictionary<SessionID, Session.Session>(acceptedSessions_);
            }
        }
    }
}