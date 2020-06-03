using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using QuickFix.Fields;
using QuickFix.Fields.Converters;
using QuickFix.StateManagement;
using QuickFix.Util;

namespace QuickFix.Session
{
    /// <summary>
    /// The Session is the primary FIX abstraction for message communication. 
    /// It performs sequencing and error recovery and represents a communication
    /// channel to a counterparty. Sessions are independent of specific communication
    /// layer connections. A Session is defined as starting with message sequence number
    /// of 1 and ending when the session is reset. The Session could span many sequential
    /// connections (it cannot operate on multiple connections simultaneously).
    /// </summary>
    public class Session : IDisposable
    {
        #region Private Members

        private static ConcurrentDictionary<SessionID, Session> sessions_ = new ConcurrentDictionary<SessionID, Session>();
         
        private object sync_ = new object();
        private IResponder responder_ = null;
        private SessionSchedule schedule_;
        private SessionState state_;
        private IMessageFactory msgFactory_;
        private bool appDoesEarlyIntercept_;
        private static readonly HashSet<string> AdminMsgTypes = new HashSet<string>() { "0", "A", "1", "2", "3", "4", "5" };
        private bool disposed_;
        private readonly AwaitableCriticalSection _awaitableCriticalSection;

        #endregion

        #region Properties

        public IConnectionState ConnectionState => state_;

        // state
        public IMessageStore MessageStore => state_.MessageStore;

        public ILog Log => state_.Log;

        //public bool IsInitiator { get { return state_.IsInitiator; } }
        //public bool IsAcceptor { get { return !state_.IsInitiator; } }
        private bool IsEnabled => state_.IsEnabled;
        public bool IsSessionTime => schedule_.IsSessionTime(DateTime.UtcNow);
        private bool IsLoggedOn => state_.ReceivedLogon && state_.SentLogon;

        public bool IsNewSession
        {
            get
            {
                var creationTime = this.state_.CreationTime;
                return !creationTime.HasValue
                    || this.schedule_.IsNewSession(creationTime.Value, DateTime.UtcNow);
            }
        }

        /// <summary>
        /// Session setting for enabling message latency checks
        /// </summary>
        public bool CheckLatency { get; private set; }

        /// <summary>
        /// Session setting for maximum message latency (in seconds)
        /// </summary>
        public int MaxLatency { get; private set; }

        /// <summary>
        /// Send a logout if counterparty times out and does not heartbeat
        /// in response to a TestRequeset. Defaults to false
        /// </summary>
        public bool SendLogoutBeforeTimeoutDisconnect { get; private set; }

        /// <summary>
        /// Gets or sets the next expected outgoing sequence number
        /// </summary>
        public int NextSenderMsgSeqNum
        {
            get
            {
                return state_.GetNextSenderMsgSeqNum();
            }
            set
            {
                state_.SetNextSenderMsgSeqNum(value);
            }
        }

        /// <summary>
        /// Gets or sets the next expected incoming sequence number
        /// </summary>
        public int NextTargetMsgSeqNum
        {
            get
            {
                return state_.GetNextTargetMsgSeqNum();
            }
            set
            {
                state_.SetNextTargetMsgSeqNum(value);
            }
        }

        /// <summary>
        /// Logon timeout in seconds
        /// </summary>
        public int LogonTimeout
        {
            get { return state_.LogonTimeout; }
            private set { state_.LogonTimeout = value; }
        }

        /// <summary>
        /// Logout timeout in seconds
        /// </summary>
        public int LogoutTimeout
        {
            get { return state_.LogoutTimeout; }
            private set { state_.LogoutTimeout = value; }
        }

        // unsynchronized properties
        /// <summary>
        /// Whether to persist messages or not. Setting to false forces quickfix 
        /// to always send GapFills instead of resending messages.
        /// </summary>
        public bool PersistMessages { get; private set; }

        /// <summary>
        /// Determines if session state should be restored from persistance
        /// layer when logging on. Useful for creating hot failover sessions.
        /// </summary>
        public bool RefreshOnLogon { get; private set; }

        /// <summary>
        /// Reset sequence numbers on logon request
        /// </summary>
        public bool ResetOnLogon { get; private set; }

        /// <summary>
        /// Reset sequence numbers to 1 after a normal logout
        /// </summary>
        public bool ResetOnLogout { get; private set; }

        /// <summary>
        /// Reset sequence numbers to 1 after abnormal termination
        /// </summary>
        public bool ResetOnDisconnect { get; private set; }

        /// <summary>
        /// Whether to send redundant resend requests
        /// </summary>
        public bool SendRedundantResendRequests { get; private set; }

        /// <summary>
        /// Whether to resend session level rejects (msg type '3') when servicing a resend request
        /// </summary>
        public bool ResendSessionLevelRejects { get; private set; }

        /// <summary>
        /// Whether to validate length and checksum of messages
        /// </summary>
        private bool ValidateLengthAndChecksum { get; set; }

        /// <summary>
        /// Validates Comp IDs for each message
        /// </summary>
        public bool CheckCompID { get; private set; }

        /// <summary>
        /// Determines if milliseconds should be added to timestamps.
        /// Only avilable on FIX4.2. or greater
        /// </summary>
        public bool MillisecondsInTimeStamp
        {
            get
            {
                return TimeStampPrecision == TimeStampPrecision.Millisecond;
            }
            private set
            {
                TimeStampPrecision = value ? TimeStampPrecision.Millisecond : TimeStampPrecision.Second;
            }
        }

        /// <summary>
        /// Gets or sets the time stamp precision.
        /// </summary>
        /// <value>
        /// The time stamp precision.
        /// </value>
        public TimeStampPrecision TimeStampPrecision
        {
            get;
            private set;
        }

        /// <summary>
        /// Adds the last message sequence number processed in the header (tag 369)
        /// </summary>
        public bool EnableLastMsgSeqNumProcessed { get; private set; }

        /// <summary>
        /// Ignores resend requests marked poss dup
        /// </summary>
        public bool IgnorePossDupResendRequests { get; private set; }

        /// <summary>
        /// Sets a maximum number of messages to request in a resend request.
        /// </summary>
        public int MaxMessagesInResendRequest { get; private set; }

        /// <summary>
        /// This is the FIX field value, e.g. "6" for FIX44
        /// </summary>
        private ApplVerID TargetDefaultApplVerID { get; set; }

        /// <summary>
        /// This is the FIX field value, e.g. "6" for FIX44
        /// </summary>
        private string SenderDefaultApplVerID { get; set; }

        public SessionID SessionID { get; set; }
        private IApplication Application { get; }
        private DataDictionaryProvider DataDictionaryProvider { get; }
        private DataDictionary.DataDictionary SessionDataDictionary { get; }
        private DataDictionary.DataDictionary ApplicationDataDictionary { get; }

        /// <summary>
        /// Returns whether the Session has a Responder. This method is synchronized
        /// </summary>
        private bool HasResponder => responder_ != default;

        /// <summary>
        /// Returns whether the Sessions will allow ResetSequence messages sent as
        /// part of a resend request (PossDup=Y) to omit the OrigSendingTime
        /// </summary>
        public bool RequiresOrigSendingTime { get; private set; }

        #endregion

        public AwaitableCriticalSection CriticalSection => _awaitableCriticalSection;

        public Session(
            IApplication app, IMessageStoreFactory storeFactory, SessionID sessID, DataDictionaryProvider dataDictProvider,
            SessionSchedule sessionSchedule, SessionConfiguration configuration, ILogFactory logFactory, IMessageFactory msgFactory, string senderDefaultApplVerID, CancellationToken cancellationToken)
        {
            this._awaitableCriticalSection = new AwaitableCriticalSection(true);
            this.Application = app;
            this.SessionID = sessID;
            this.DataDictionaryProvider = new DataDictionaryProvider(dataDictProvider);
            this.schedule_ = sessionSchedule;
            this.msgFactory_ = msgFactory;
            this.appDoesEarlyIntercept_ = app is IApplicationExt;

            this.SenderDefaultApplVerID = senderDefaultApplVerID;

            this.SessionDataDictionary = this.DataDictionaryProvider.GetSessionDataDictionary(this.SessionID.BeginString);
            if (this.SessionID.IsFIXT)
                this.ApplicationDataDictionary = this.DataDictionaryProvider.GetApplicationDataDictionary(this.SenderDefaultApplVerID);
            else
                this.ApplicationDataDictionary = this.SessionDataDictionary;

            ILog log;
            if (null != logFactory)
                log = logFactory.Create(sessID);
            else
                log = new NullLog();

            state_ = new SessionState(log, configuration.HeartBtInt)
            {
                MessageStore = storeFactory.Create(sessID)
            };
            InnerInitialize(configuration);
            

            if (!IsSessionTime)
                Reset("Out of SessionTime (Session construction)", cancellationToken).GetAwaiter().GetResult();
            else if (IsNewSession)
                Reset("New session", cancellationToken).GetAwaiter().GetResult();

            lock (sessions_)
            {
                sessions_[this.SessionID] = this;
            }

            this.Application.OnCreate(this.SessionID);
            this.Log.OnEvent("Created session");
        }

        public async Task Initialize(SessionConfiguration configuration, CancellationToken cancellationToken)
        {
            using (await _awaitableCriticalSection.EnterAsync(cancellationToken))
            {
                InnerInitialize(configuration);
            }
        } 

        private void InnerInitialize(SessionConfiguration configuration)
        {
            if (configuration.CheckCompId.HasValue) CheckCompID = configuration.CheckCompId.Value;
            if (configuration.CheckLatency.HasValue) CheckLatency = configuration.CheckLatency.Value;
            if (configuration.MaxLatency.HasValue) MaxLatency = configuration.MaxLatency.Value;
            if (configuration.LogonTimeout.HasValue) LogonTimeout = configuration.LogonTimeout.Value;
            if (configuration.LogoutTimeout.HasValue) LogoutTimeout = configuration.LogoutTimeout.Value;
            if (configuration.ResetOnLogon.HasValue) ResetOnLogon = configuration.ResetOnLogon.Value;
            if (configuration.ResetOnLogout.HasValue) ResetOnLogout = configuration.ResetOnLogout.Value;
            if (configuration.ResetOnDisconnect.HasValue) ResetOnDisconnect = configuration.ResetOnDisconnect.Value;
            if (configuration.RefreshOnLogon.HasValue) RefreshOnLogon = configuration.RefreshOnLogon.Value;
            if (configuration.PersistMessages.HasValue) PersistMessages = configuration.PersistMessages.Value;
            if (configuration.MillisecondsInTimeStamp.HasValue) MillisecondsInTimeStamp = configuration.MillisecondsInTimeStamp.Value;
            if (configuration.TimeStampPrecision.HasValue) TimeStampPrecision = configuration.TimeStampPrecision.Value;
            if (configuration.EnableLastMsgSeqNumProcessed.HasValue) EnableLastMsgSeqNumProcessed = configuration.EnableLastMsgSeqNumProcessed.Value;
            if (configuration.MaxMessagesInResendRequest.HasValue) MaxMessagesInResendRequest = configuration.MaxMessagesInResendRequest.Value;
            if (configuration.SendLogoutBeforeTimeoutDisconnect.HasValue) SendLogoutBeforeTimeoutDisconnect = configuration.SendLogoutBeforeTimeoutDisconnect.Value;
            if (configuration.IgnorePossDupResendRequests.HasValue) IgnorePossDupResendRequests = configuration.IgnorePossDupResendRequests.Value;
            if (configuration.ValidateLengthAndChecksum.HasValue) ValidateLengthAndChecksum = configuration.ValidateLengthAndChecksum.Value;
            if (configuration.RequiresOrigSendingTime.HasValue) RequiresOrigSendingTime = configuration.RequiresOrigSendingTime.Value;
            if (configuration.SendRedundantResendRequests.HasValue) SendRedundantResendRequests = configuration.SendRedundantResendRequests.Value;
            if (configuration.ResendSessionLevelRejects.HasValue) ResendSessionLevelRejects = configuration.ResendSessionLevelRejects.Value;
        }


        #region Static Methods

        /// <summary>
        /// Looks up a Session by its SessionID
        /// </summary>
        /// <param name="sessionID">the SessionID of the Session</param>
        /// <returns>the Session if found, else returns null</returns>
        public static Session LookupSession(SessionID sessionID)
        {
            Session result;
            lock (sessions_)
            {
                if (!sessions_.TryGetValue(sessionID, out result))
                    result = null;
            }
            return result;
        }

        /// <summary>
        /// Looks up a Session by its SessionID
        /// </summary>
        /// <param name="sessionID">the SessionID of the Session</param>
        /// <returns>the true if Session exists, false otherwise</returns>
        public static bool DoesSessionExist(SessionID sessionID)
        {
            return LookupSession(sessionID) == null ? false : true;
        }

        /// <summary>
        /// Sends a message to the session specified by the provider session ID.
        /// </summary>
        /// <param name="message">FIX message</param>
        /// <param name="sessionID">target SessionID</param>
        /// <param name="cancellationToken"></param>
        /// <returns>true if send was successful, false otherwise</returns>
        public static Task<bool> SendToTarget(Message message, SessionID sessionID, CancellationToken cancellationToken)
        {
            message.SetSessionID(sessionID);
            Session session = Session.LookupSession(sessionID);
            if (null == session)
                throw new SessionNotFound(sessionID);
            return session.Send(message, cancellationToken);
        }

        /// <summary>
        /// Send to session indicated by header fields in message
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static Task<bool> SendToTarget(Message message, CancellationToken cancellationToken)
        {
            SessionID sessionID = message.GetSessionID(message);
            return SendToTarget(message, sessionID, cancellationToken);
        }

        #endregion

        public async Task<SessionDetails> GetDetails(CancellationToken cancellationToken)
        {
            using (await _awaitableCriticalSection.EnterAsync(cancellationToken).ConfigureAwait(false))
            {
                return new SessionDetails
                {
                    IsInitiator = state_.IsInitiator,
                    IsLoggedOn = IsLoggedOn,
                    HeartBtInt =  state_.HeartBtInt,
                    IsEnabled = state_.IsEnabled,
                    HasResponder = HasResponder
                };
            }
        }

        /// <summary>
        /// Sends a message via the session indicated by the header fields
        /// </summary>
        /// <param name="message">message to send</param>
        /// <param name="cancellationToken"></param>
        /// <returns>true if was sent successfully</returns>
        public virtual async Task<bool> Send(Message message, CancellationToken cancellationToken)
        {
            using (await _awaitableCriticalSection.EnterAsync(cancellationToken))
            {
                message.Header.RemoveField(Fields.Tags.PossDupFlag);
                message.Header.RemoveField(Fields.Tags.OrigSendingTime);
                return SendRaw(message, 0);
            }
        }

        /// <summary>
        /// Sends a message
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        private bool Send(string message)
        {
            if (!HasResponder)
                return false;
            this.Log.OnOutgoing(message);
            return responder_.Send(message);
        }

        // TODO for v2 - rename, make internal
        /// <summary>
        /// Sets some internal state variables.  Despite the name, it does not do anything to make a logon occur.
        /// </summary>
        public void Logon()
        {
            state_.IsEnabled = true;
            state_.LogoutReason = string.Empty;
        }

        // TODO for v2 - rename, make internal
        /// <summary>
        /// Sets some internal state variables.  Despite the name, it does not cause a logout to occur.
        /// </summary>
        public void Logout()
        {
            Logout(string.Empty);
        }

        // TODO for v2 - rename, make internal
        /// <summary>
        /// Sets some internal state variables.  Despite the name, it does not cause a logout to occur.
        /// </summary>
        private void Logout(string reason)
        {
            state_.IsEnabled = false;
            state_.LogoutReason = reason;
        }

        /// <summary>
        /// Logs out from session and closes the network connection
        /// </summary>
        /// <param name="reason"></param>
        public async Task Disconnect(string reason, CancellationToken cancellationToken)
        {
            //TODO: should be revise all places where this method called
            using(await _awaitableCriticalSection.EnterAsync(cancellationToken))
            {
                if (null != responder_)
                {
                    this.Log.OnEvent("Session " + this.SessionID + " disconnecting: " + reason);
                    responder_.Disconnect();
                    responder_ = null;
                }
                else
                {
                    this.Log.OnEvent("Session " + this.SessionID + " already disconnected: " + reason);
                }

                if (state_.ReceivedLogon || state_.SentLogon)
                {
                    state_.ReceivedLogon = false;
                    state_.SentLogon = false;
                    this.Application.OnLogout(this.SessionID);
                }

                state_.SentLogout = false;
                state_.ReceivedReset = false;
                state_.SentReset = false;
                state_.ClearQueue();
                state_.LogoutReason = "";
                if (this.ResetOnDisconnect)
                    state_.Reset("ResetOnDisconnect");
                state_.SetResendRange(0, 0);
            }
        }

        /// <summary>
        /// There's no message to process, but check the session state to see if there's anything to do
        /// (e.g. send heartbeat, logout at end of session, etc)
        /// </summary>
        public async Task Next(CancellationToken cancellationToken)
        {
            //session is a root object that heavy use by thread so should be sync just session and mesages threads
            using (await _awaitableCriticalSection.EnterAsync(cancellationToken))
            {
                if (!HasResponder)
                    return;

                if (!IsSessionTime)
                {
                    if (!state_.IsInitiator)
                        await Reset("Out of SessionTime (Session.Next())", "Message received outside of session time", cancellationToken);
                    else
                        await Reset("Out of SessionTime (Session.Next())", cancellationToken);
                    return;
                }

                if (IsNewSession)
                    state_.Reset("New session (detected in Next())");

                if (!IsEnabled)
                {
                    if (!IsLoggedOn)
                        return;

                    if (!state_.SentLogout)
                    {
                        this.Log.OnEvent("Initiated logout request");
                        GenerateLogout(state_.LogoutReason);
                    }
                }

                if (!state_.ReceivedLogon)
                {
                    if (state_.ShouldSendLogon && IsTimeToGenerateLogon())
                    {
                        if (GenerateLogon())
                            this.Log.OnEvent("Initiated logon request");
                        else
                            this.Log.OnEvent("Error during logon request initiation");

                    }
                    else if (state_.SentLogon && state_.LogonTimedOut())
                    {
                        await Disconnect("Timed out waiting for logon response", cancellationToken);
                    }
                    return;
                }

                if (0 == state_.HeartBtInt)
                    return;


                if (state_.LogoutTimedOut())
                    await Disconnect("Timed out waiting for logout response", cancellationToken);


                if (state_.WithinHeartbeat())
                    return;

                if (state_.TimedOut())
                {
                    if (this.SendLogoutBeforeTimeoutDisconnect)
                        GenerateLogout();
                    await Disconnect("Timed out waiting for heartbeat", cancellationToken);
                }
                else
                {
                    if (state_.NeedTestRequest())
                    {

                        GenerateTestRequest("TEST");
                        state_.TestRequestCounter += 1;
                        this.Log.OnEvent("Sent test request TEST");
                    }
                    else if (state_.NeedHeartbeat())
                    {
                        GenerateHeartbeat();
                    }
                }
            }
        }

        /// <summary>
        /// Process a message (in string form) from the counterparty
        /// </summary>
        /// <param name="msgStr"></param>
        /// <param name="cancellationToken"></param>
        public async Task Next(string msgStr, CancellationToken cancellationToken)
        {
            //session is a root object that heavy use by thread so should be sync just session and mesages threads
            using (await _awaitableCriticalSection.EnterAsync(cancellationToken))
            {
                await NextMessage(msgStr, cancellationToken);
                await NextQueued(cancellationToken);
            }
        }

        /// <summary>
        /// Process a message (in string form) from the counterparty
        /// </summary>
        /// <param name="msgStr"></param>
        /// <param name="cancellationToken"></param>
        private Task NextMessage(string msgStr, CancellationToken cancellationToken)
        {
            Log.OnIncoming(msgStr);

            var msgBuilder = new MessageBuilder(
                    msgStr,
                    SenderDefaultApplVerID,
                    ValidateLengthAndChecksum,
                    SessionDataDictionary,
                    ApplicationDataDictionary,
                    msgFactory_);

            return Next(msgBuilder, cancellationToken);
        }

        /// <summary>
        /// Process a message from the counterparty.
        /// </summary>
        /// <param name="msgBuilder"></param>
        /// <param name="cancellationToken"></param>
        private async Task Next(MessageBuilder msgBuilder, CancellationToken cancellationToken)
        {
            if (!IsSessionTime)
            {
                await Reset("Out of SessionTime (Session.Next(message))", "Message received outside of session time", cancellationToken);
                return;
            }

            if (IsNewSession)
                state_.Reset("New session (detected in Next(Message))");

            Message message = null; // declared outside of try-block so that catch-blocks can use it

            try
            {
                message = msgBuilder.Build();

                if (appDoesEarlyIntercept_)
                    ((IApplicationExt)Application).FromEarlyIntercept(message, this.SessionID);

                Header header = message.Header;
                string msgType = msgBuilder.MsgType.Obj;
                string beginString = msgBuilder.BeginString;

                if (!beginString.Equals(this.SessionID.BeginString))
                    throw new UnsupportedVersion(beginString);


                if (MsgType.LOGON.Equals(msgType))
                {
                    if (this.SessionID.IsFIXT)
                    {
                        TargetDefaultApplVerID = new ApplVerID(message.GetString(Fields.Tags.DefaultApplVerID));
                    }
                    else
                    {
                        TargetDefaultApplVerID = Message.GetApplVerID(beginString);
                    }
                }

                if (this.SessionID.IsFIXT && !Message.IsAdminMsgType(msgType))
                {
                    DataDictionary.DataDictionary.Validate(message, SessionDataDictionary, ApplicationDataDictionary, beginString, msgType);
                }
                else
                {
                    this.SessionDataDictionary.Validate(message, beginString, msgType);
                }


                if (MsgType.LOGON.Equals(msgType))
                    await NextLogon(message, cancellationToken);
                else if (!IsLoggedOn)
                    await Disconnect($"Received msg type '{msgType}' when not logged on", cancellationToken);
                else if (MsgType.HEARTBEAT.Equals(msgType))
                    await NextHeartbeat(message, cancellationToken);
                else if (MsgType.TEST_REQUEST.Equals(msgType))
                    await NextTestRequest(message, cancellationToken);
                else if (MsgType.SEQUENCE_RESET.Equals(msgType))
                    await NextSequenceReset(message, cancellationToken);
                else if (MsgType.LOGOUT.Equals(msgType))
                    await NextLogout(message, cancellationToken);
                else if (MsgType.RESEND_REQUEST.Equals(msgType))
                    await NextResendRequest(message, cancellationToken);
                else
                {
                    if (! await Verify(message, cancellationToken))
                        return;
                    state_.IncrNextTargetMsgSeqNum();
                }

            }
            catch (InvalidMessage e)
            {
                this.Log.OnEvent(e.Message);

                try
                {
                    if (MsgType.LOGON.Equals(msgBuilder.MsgType.Obj))
                        await Disconnect("Logon message is not valid", cancellationToken);
                }
                catch (MessageParseError)
                { }

                throw e;
            }
            catch (TagException e)
            {
                if (null != e.InnerException)
                    this.Log.OnEvent(e.InnerException.Message);
                GenerateReject(msgBuilder, e.sessionRejectReason, e.Field);
            }
            catch (UnsupportedVersion uvx)
            {
                if (MsgType.LOGOUT.Equals(msgBuilder.MsgType.Obj))
                {
                    await NextLogout(message, cancellationToken);
                }
                else
                {
                    this.Log.OnEvent(uvx.ToString());
                    GenerateLogout(uvx.Message);
                    state_.IncrNextTargetMsgSeqNum();
                }
            }
            catch (UnsupportedMessageType e)
            {
                this.Log.OnEvent("Unsupported message type: " + e.Message);
                GenerateBusinessMessageReject(message, Fields.BusinessRejectReason.UNKNOWN_MESSAGE_TYPE, 0);
            }
            catch (FieldNotFoundException e)
            {
                this.Log.OnEvent("Rejecting invalid message, field not found: " + e.Message);
                if ((SessionID.BeginString.CompareTo(FixValues.BeginString.FIX42) >= 0) && (message.IsApp()))
                {
                    GenerateBusinessMessageReject(message, Fields.BusinessRejectReason.CONDITIONALLY_REQUIRED_FIELD_MISSING, e.Field);
                }
                else
                {
                    if (MsgType.LOGON.Equals(msgBuilder.MsgType.Obj))
                    {
                        this.Log.OnEvent("Required field missing from logon");
                        await Disconnect("Required field missing from logon", cancellationToken);
                    }
                    else
                        GenerateReject(msgBuilder, new QuickFix.FixValues.SessionRejectReason(SessionRejectReason.REQUIRED_TAG_MISSING, "Required Tag Missing"), e.Field);
                }
            }
            catch (RejectLogon e)
            {
                GenerateLogout(e.Message);
                await Disconnect(e.ToString(), cancellationToken);
            }
            
            //we already started dedicated task for client session service, don't need to do this here.
            //TODO: dedicated task should be started for server
            //Next(); 

        }

        private async Task NextLogon(Message logon, CancellationToken cancellationToken)
        {
            Fields.ResetSeqNumFlag resetSeqNumFlag = new Fields.ResetSeqNumFlag(false);
            if (logon.IsSetField(resetSeqNumFlag))
                logon.GetField(resetSeqNumFlag);
            state_.ReceivedReset = resetSeqNumFlag.Obj;

            if (state_.ReceivedReset)
            {
                this.Log.OnEvent("Sequence numbers reset due to ResetSeqNumFlag=Y");
                if (!state_.SentReset)
                {
                    state_.Reset("Reset requested by counterparty");
                }
            }

            if (!state_.IsInitiator && this.ResetOnLogon)
                state_.Reset("ResetOnLogon");
            if (this.RefreshOnLogon)
                InnerRefresh();

            if (! await Verify(logon, false, true, cancellationToken))
                return;

            if (!IsGoodTime(logon))
            {
                this.Log.OnEvent("Logon has bad sending time");
                await Disconnect("bad sending time", cancellationToken);
                return;
            }

            state_.ReceivedLogon = true;
            this.Log.OnEvent("Received logon");
            if (!state_.IsInitiator)
            {
                int heartBtInt = logon.GetInt(Fields.Tags.HeartBtInt);
                state_.HeartBtInt = heartBtInt;
                GenerateLogon(logon);
                this.Log.OnEvent("Responding to logon request");
            }

            state_.SentReset = false;
            state_.ReceivedReset = false;

            int msgSeqNum = logon.Header.GetInt(Fields.Tags.MsgSeqNum);
            if (IsTargetTooHigh(msgSeqNum) && !resetSeqNumFlag.Obj)
            {
                DoTargetTooHigh(logon, msgSeqNum);
            }
            else
            {
                state_.IncrNextTargetMsgSeqNum();
            }

            if (this.IsLoggedOn)
                this.Application.OnLogon(this.SessionID);
        }

        private async Task NextTestRequest(Message testRequest, CancellationToken cancellationToken)
        {
            if (! await Verify(testRequest, cancellationToken))
                return;
            GenerateHeartbeat(testRequest);
            state_.IncrNextTargetMsgSeqNum();
        }

        private async Task NextResendRequest(Message resendReq, CancellationToken cancellationToken)
        {
            if (! await Verify(resendReq, false, false, cancellationToken))
                return;
            try
            {
                int msgSeqNum = 0;
                if (!(this.IgnorePossDupResendRequests && resendReq.Header.IsSetField(Tags.PossDupFlag)))
                {
                    int begSeqNo = resendReq.GetInt(Fields.Tags.BeginSeqNo);
                    int endSeqNo = resendReq.GetInt(Fields.Tags.EndSeqNo);
                    this.Log.OnEvent("Got resend request from " + begSeqNo + " to " + endSeqNo);

                    if ((endSeqNo == 999999) || (endSeqNo == 0))
                    {
                        endSeqNo = state_.GetNextSenderMsgSeqNum() - 1;
                    }

                    if (!PersistMessages)
                    {
                        endSeqNo++;
                        int next = state_.GetNextSenderMsgSeqNum();
                        if (endSeqNo > next)
                            endSeqNo = next;
                        GenerateSequenceReset(resendReq, begSeqNo, endSeqNo);
                        msgSeqNum = resendReq.Header.GetInt(Tags.MsgSeqNum);
                        if (!IsTargetTooHigh(msgSeqNum) && !IsTargetTooLow(msgSeqNum))
                        {
                            state_.IncrNextTargetMsgSeqNum();
                        }
                        return;
                    }

                    List<string> messages = new List<string>();
                    state_.Get(begSeqNo, endSeqNo, messages);
                    int current = begSeqNo;
                    int begin = 0;
                    foreach (string msgStr in messages)
                    {
                        Message msg = new Message();
                        msg.FromString(msgStr, true, this.SessionDataDictionary, this.ApplicationDataDictionary, msgFactory_);
                        msgSeqNum = msg.Header.GetInt(Tags.MsgSeqNum);

                        if ((current != msgSeqNum) && begin == 0)
                        {
                            begin = current;
                        }

                        if (IsAdminMessage(msg) && !(this.ResendSessionLevelRejects && msg.Header.GetString(Tags.MsgType) == MsgType.REJECT))
                        {
                            if (begin == 0)
                            {
                                begin = msgSeqNum;
                            }
                        }
                        else
                        {

                            InitializeResendFields(msg);
                            if(!ResendApproved(msg, SessionID)) 
                            {
                                continue;
                            }

                            if (begin != 0)
                            {
                                GenerateSequenceReset(resendReq, begin, msgSeqNum);
                            }
                            Send(msg.ToString());
                            begin = 0;
                        }
                        current = msgSeqNum + 1;
                    }

                    int nextSeqNum = state_.GetNextSenderMsgSeqNum();
                    if (++endSeqNo > nextSeqNum)
                    {
                        endSeqNo = nextSeqNum;
                    }

                    if (begin == 0)
                    {
                        begin = current;
                    }

                    if (endSeqNo > begin)
                    {
                        GenerateSequenceReset(resendReq, begin, endSeqNo);
                    }
                }
                msgSeqNum = resendReq.Header.GetInt(Tags.MsgSeqNum);
                if (!IsTargetTooHigh(msgSeqNum) && !IsTargetTooLow(msgSeqNum))
                {
                    state_.IncrNextTargetMsgSeqNum();
                }

            }
            catch (System.Exception e)
            {
                this.Log.OnEvent("ERROR during resend request " + e.Message);
            }
        }
        private bool ResendApproved(Message msg, SessionID sessionID)
        {
            try
            {
                Application.ToApp(msg, sessionID);
            }
            catch (DoNotSend)
            {
                return false;
            }

            return true;
        }

        private async Task NextLogout(Message logout, CancellationToken cancellationToken)
        {
            if (! await Verify(logout, false, false, cancellationToken))
                return;

            string disconnectReason;

            if (!state_.SentLogout)
            {
                disconnectReason = "Received logout request";
                this.Log.OnEvent(disconnectReason);
                GenerateLogout(logout);
                this.Log.OnEvent("Sending logout response");
            }
            else
            {
                disconnectReason = "Received logout response";
                this.Log.OnEvent(disconnectReason);
            }

            state_.IncrNextTargetMsgSeqNum();
            if (this.ResetOnLogout)
                state_.Reset("ResetOnLogout");
            await Disconnect(disconnectReason, cancellationToken);
        }

        private async Task NextHeartbeat(Message heartbeat, CancellationToken cancellationToken)
        {
            if (! await Verify(heartbeat, cancellationToken))
                return;
            state_.IncrNextTargetMsgSeqNum();
        }

        private async Task NextSequenceReset(Message sequenceReset, CancellationToken cancellationToken)
        {
            bool isGapFill = false;
            if (sequenceReset.IsSetField(Fields.Tags.GapFillFlag))
                isGapFill = sequenceReset.GetBoolean(Fields.Tags.GapFillFlag);

            if (! await Verify(sequenceReset, isGapFill, isGapFill, cancellationToken))
                return;

            if (sequenceReset.IsSetField(Fields.Tags.NewSeqNo))
            {
                int newSeqNo = sequenceReset.GetInt(Fields.Tags.NewSeqNo);
                this.Log.OnEvent("Received SequenceReset FROM: " + state_.GetNextTargetMsgSeqNum() + " TO: " + newSeqNo);

                if (newSeqNo > state_.GetNextTargetMsgSeqNum())
                {
                    state_.SetNextTargetMsgSeqNum(newSeqNo);
                }
                else
                {
                    if (newSeqNo < state_.GetNextTargetMsgSeqNum())
                        GenerateReject(sequenceReset, FixValues.SessionRejectReason.VALUE_IS_INCORRECT);
                }
            }
        }

        private Task<bool> Verify(Message message, CancellationToken cancellationToken) => Verify(message, true, true, cancellationToken);

        public async Task<bool> Verify(Message msg, bool checkTooHigh, bool checkTooLow, CancellationToken cancellationToken)
        {
            int msgSeqNum = 0;
            string msgType = "";

            try
            {
                msgType = msg.Header.GetString(Fields.Tags.MsgType);
                string senderCompID = msg.Header.GetString(Fields.Tags.SenderCompID);
                string targetCompID = msg.Header.GetString(Fields.Tags.TargetCompID);

                if (!IsCorrectCompId(senderCompID, targetCompID))
                {
                    GenerateReject(msg, FixValues.SessionRejectReason.COMPID_PROBLEM);
                    GenerateLogout();
                    return false;
                }

                if (checkTooHigh || checkTooLow)
                    msgSeqNum = msg.Header.GetInt(Fields.Tags.MsgSeqNum);

                if (checkTooHigh && IsTargetTooHigh(msgSeqNum))
                {
                    DoTargetTooHigh(msg, msgSeqNum);
                    return false;
                }
                else if (checkTooLow && IsTargetTooLow(msgSeqNum))
                {
                    DoTargetTooLow(msg, msgSeqNum);
                    return false;
                }

                if ((checkTooHigh || checkTooLow) && state_.ResendRequested())
                {
                    ResendRange range = state_.GetResendRange();
                    if (msgSeqNum >= range.EndSeqNo)
                    {
                        this.Log.OnEvent("ResendRequest for messages FROM: " + range.BeginSeqNo + " TO: " + range.EndSeqNo + " has been satisfied.");
                        state_.SetResendRange(0, 0);
                    }
                    else if (msgSeqNum >= range.ChunkEndSeqNo)
                    {
                        this.Log.OnEvent("Chunked ResendRequest for messages FROM: " + range.BeginSeqNo + " TO: " + range.ChunkEndSeqNo + " has been satisfied.");
                        int newChunkEndSeqNo = Math.Min(range.EndSeqNo, range.ChunkEndSeqNo + this.MaxMessagesInResendRequest);
                        GenerateResendRequestRange(msg.Header.GetString(Fields.Tags.BeginString), range.ChunkEndSeqNo + 1, newChunkEndSeqNo);
                        range.ChunkEndSeqNo = newChunkEndSeqNo;
                    }
                }

                if (!IsGoodTime(msg))
                {
                    this.Log.OnEvent("Sending time accuracy problem");
                    GenerateReject(msg, FixValues.SessionRejectReason.SENDING_TIME_ACCURACY_PROBLEM);
                    GenerateLogout();
                    return false;
                }
            }
            catch (System.Exception e)
            {
                this.Log.OnEvent("Verify failed: " + e.Message);
                await Disconnect("Verify failed: " + e.Message, cancellationToken);
                return false;
            }

            state_.LastReceivedTimeDT = DateTime.UtcNow;
            state_.TestRequestCounter = 0;

            if (Message.IsAdminMsgType(msgType))
                this.Application.FromAdmin(msg, this.SessionID);
            else
                this.Application.FromApp(msg, this.SessionID);

            return true;
        }

        public async Task SetResponder(IResponder responder, CancellationToken cancellationToken)
        {
            using (await _awaitableCriticalSection.EnterAsync(cancellationToken))
            {
                if (!IsSessionTime)
                     await Reset("Out of SessionTime (Session.SetResponder)", null, cancellationToken);
                responder_ = responder;
            }
        }

        public async Task Refresh( CancellationToken cancellationToken)
        {
            using(await _awaitableCriticalSection.EnterAsync(cancellationToken))
                InnerRefresh();
        }

        /// <summary>
        /// Send a logout, disconnect, and reset session state
        /// </summary>
        /// <param name="loggedReason">reason for the reset (for the log)</param>
        /// <param name="cancellationToken"></param>
        public async Task Reset(string loggedReason, CancellationToken cancellationToken)
        {
            using(await _awaitableCriticalSection.EnterAsync(cancellationToken))
                await Reset(loggedReason, null, cancellationToken);
        }

        private void InnerRefresh() => state_.Refresh();

        /// <summary>
        /// Send a logout, disconnect, and reset session state
        /// </summary>
        /// <param name="loggedReason">reason for the reset (for the log)</param>
        /// <param name="logoutMessage">message to put in the Logout message's Text field (ignored if null/empty string)</param>
        /// <param name="cancellationToken"></param>
        private async Task Reset(string loggedReason, string logoutMessage, CancellationToken cancellationToken)
        {
            if(this.IsLoggedOn)
                GenerateLogout(logoutMessage);
            await Disconnect("Resetting...", cancellationToken);
            state_.Reset(loggedReason);
        }

        private void InitializeResendFields(Message message)
        {
            FieldMap header = message.Header;
            System.DateTime sendingTime = header.GetDateTime(Fields.Tags.SendingTime);
            InsertOrigSendingTime(header, sendingTime);
            header.SetField(new Fields.PossDupFlag(true));
            InsertSendingTime(header);
        }

        private bool ShouldSendReset()
        {
            return (this.SessionID.BeginString.CompareTo(FixValues.BeginString.FIX41) >= 0)
                && (this.ResetOnLogon || this.ResetOnLogout || this.ResetOnDisconnect)
                && (state_.GetNextSenderMsgSeqNum() == 1)
                && (state_.GetNextTargetMsgSeqNum() == 1);
        }

        private bool IsCorrectCompId(string senderCompId, string targetCompId)
        {
            if (!this.CheckCompID)
                return true;
            return this.SessionID.SenderCompID.Equals(targetCompId)
                && this.SessionID.TargetCompID.Equals(senderCompId);
        }

        /// FIXME
        private bool IsTimeToGenerateLogon()
        {
            return true;
        }

        private bool IsTargetTooHigh(int msgSeqNum)
        {
            return msgSeqNum > state_.GetNextTargetMsgSeqNum();
        }

        private bool IsTargetTooLow(int msgSeqNum)
        {
            return msgSeqNum < state_.GetNextTargetMsgSeqNum();
        }

        private void DoTargetTooHigh(Message msg, int msgSeqNum)
        {
            string beginString = msg.Header.GetString(Fields.Tags.BeginString);

            this.Log.OnEvent("MsgSeqNum too high, expecting " + state_.GetNextTargetMsgSeqNum() + " but received " + msgSeqNum);
            state_.Queue(msgSeqNum, msg);

            if (state_.ResendRequested())
            {
                ResendRange range = state_.GetResendRange();

                if (!this.SendRedundantResendRequests && msgSeqNum >= range.BeginSeqNo)
                {
                    this.Log.OnEvent("Already sent ResendRequest FROM: " + range.BeginSeqNo + " TO: " + range.EndSeqNo + ".  Not sending another.");
                    return;
                }
            }

            GenerateResendRequest(beginString, msgSeqNum);
        }

        private void DoTargetTooLow(Message msg, int msgSeqNum)
        {
            bool possDupFlag = false;
            if (msg.Header.IsSetField(Fields.Tags.PossDupFlag))
                possDupFlag = msg.Header.GetBoolean(Fields.Tags.PossDupFlag);

            if (!possDupFlag)
            {
                string err = "MsgSeqNum too low, expecting " + state_.GetNextTargetMsgSeqNum() + " but received " + msgSeqNum;
                GenerateLogout(err);
                throw new QuickFIXException(err);
            }

            DoPossDup(msg);
        }

        /// <summary>
        /// Validates a message where PossDupFlag=Y
        /// </summary>
        /// <param name="msg"></param>
        private void DoPossDup(Message msg)
        {
            // If config RequiresOrigSendingTime=N, then tolerate SequenceReset messages that lack OrigSendingTime (issue #102).
            // (This field doesn't really make sense in this message, so some parties omit it, even though spec requires it.)
            string msgType = msg.Header.GetString(Fields.Tags.MsgType); 
            if (msgType == Fields.MsgType.SEQUENCE_RESET && RequiresOrigSendingTime == false)
                return;

            // Reject if messages don't have OrigSendingTime set
            if (!msg.Header.IsSetField(Fields.Tags.OrigSendingTime))
            {
                GenerateReject(msg, FixValues.SessionRejectReason.REQUIRED_TAG_MISSING, Fields.Tags.OrigSendingTime);
                return;
            }

            // Ensure sendingTime is later than OrigSendingTime, else reject and logout
            DateTime origSendingTime = msg.Header.GetDateTime(Fields.Tags.OrigSendingTime);
            DateTime sendingTime = msg.Header.GetDateTime(Fields.Tags.SendingTime);
            System.TimeSpan tmSpan = origSendingTime - sendingTime;

            if (tmSpan.TotalSeconds > 0)
            {
                GenerateReject(msg, FixValues.SessionRejectReason.SENDING_TIME_ACCURACY_PROBLEM);
                GenerateLogout();
            }
        }

        private void GenerateBusinessMessageReject(Message message, int err, int field)
        {
            string msgType = message.Header.GetString(Tags.MsgType);
            int msgSeqNum = message.Header.GetInt(Tags.MsgSeqNum);
            string reason = FixValues.BusinessRejectReason.RejText[err];
            Message reject;
            if (this.SessionID.BeginString.CompareTo(FixValues.BeginString.FIX42) >= 0)
            {
                reject = msgFactory_.Create(this.SessionID.BeginString, MsgType.BUSINESS_MESSAGE_REJECT);
                reject.SetField(new RefMsgType(msgType));
                reject.SetField(new BusinessRejectReason(err));
            }
            else
            {
                reject = msgFactory_.Create(this.SessionID.BeginString, MsgType.REJECT);
                char[] reasonArray = reason.ToLower().ToCharArray();
                reasonArray[0] = char.ToUpper(reasonArray[0]);
                reason = new string(reasonArray);
            }
            InitializeHeader(reject);
            reject.SetField(new RefSeqNum(msgSeqNum));
            state_.IncrNextTargetMsgSeqNum();


            reject.SetField(new Text(reason));
            Log.OnEvent("Reject sent for Message: " + msgSeqNum + " Reason:" + reason);
            SendRaw(reject, 0);
        }

        private bool GenerateResendRequestRange(string beginString, int startSeqNum, int endSeqNum)
        {
            Message resendRequest = msgFactory_.Create(beginString, MsgType.RESEND_REQUEST);

            resendRequest.SetField(new Fields.BeginSeqNo(startSeqNum));
            resendRequest.SetField(new Fields.EndSeqNo(endSeqNum));

            InitializeHeader(resendRequest);
            if (SendRaw(resendRequest, 0))
            {
                this.Log.OnEvent("Sent ResendRequest FROM: " + startSeqNum + " TO: " + endSeqNum);
                return true;
            }
            else
            {
                this.Log.OnEvent("Error sending ResendRequest (" + startSeqNum + " ," + endSeqNum + ")");
                return false;
            }
        }

        private bool GenerateResendRequest(string beginString, int msgSeqNum)
        {
            int beginSeqNum = state_.GetNextTargetMsgSeqNum();
            int endRangeSeqNum = msgSeqNum - 1;
            int endChunkSeqNum;
            if (this.MaxMessagesInResendRequest > 0)
            {
                endChunkSeqNum = Math.Min(endRangeSeqNum, beginSeqNum + this.MaxMessagesInResendRequest - 1);
            }
            else
            {
                if (beginString.CompareTo(FixValues.BeginString.FIX42) >= 0)
                    endRangeSeqNum = 0;
                else if (beginString.CompareTo(FixValues.BeginString.FIX41) <= 0)
                    endRangeSeqNum = 999999;
                endChunkSeqNum = endRangeSeqNum;
            }

            if (!GenerateResendRequestRange(beginString, beginSeqNum, endChunkSeqNum))
            {
                return false;
            }

            state_.SetResendRange(beginSeqNum, endRangeSeqNum, endChunkSeqNum);
            return true;
        }

        /// <summary>
        /// FIXME
        /// </summary>
        /// <returns></returns>
        private bool GenerateLogon()
        {
            Message logon = msgFactory_.Create(this.SessionID.BeginString, Fields.MsgType.LOGON);
            logon.SetField(new Fields.EncryptMethod(0));
            logon.SetField(new Fields.HeartBtInt(state_.HeartBtInt));

            if (this.SessionID.IsFIXT)
                logon.SetField(new Fields.DefaultApplVerID(this.SenderDefaultApplVerID));
            if (this.RefreshOnLogon)
                InnerRefresh();
            if (this.ResetOnLogon)
                state_.Reset("ResetOnLogon");
            if (ShouldSendReset())
                logon.SetField(new Fields.ResetSeqNumFlag(true));

            InitializeHeader(logon);
            state_.LastReceivedTimeDT = DateTime.UtcNow;
            state_.TestRequestCounter = 0;
            state_.SentLogon = true;
            return SendRaw(logon, 0);
        }

        /// <summary>
        /// FIXME don't do so much operator new here
        /// </summary>
        /// <param name="otherLogon"></param>
        /// <returns></returns>
        private bool GenerateLogon(Message otherLogon)
        {
            Message logon = msgFactory_.Create(this.SessionID.BeginString, Fields.MsgType.LOGON);
            logon.SetField(new Fields.EncryptMethod(0));
            if (this.SessionID.IsFIXT)
                logon.SetField(new Fields.DefaultApplVerID(this.SenderDefaultApplVerID));
            logon.SetField(new Fields.HeartBtInt(otherLogon.GetInt(Tags.HeartBtInt)));
            if (this.EnableLastMsgSeqNumProcessed)
                logon.Header.SetField(new Fields.LastMsgSeqNumProcessed(otherLogon.Header.GetInt(Tags.MsgSeqNum)));

            InitializeHeader(logon);
            state_.SentLogon = SendRaw(logon, 0);
            return state_.SentLogon;
        }

        private bool GenerateTestRequest(string id)
        {
            Message testRequest = msgFactory_.Create(this.SessionID.BeginString, Fields.MsgType.TEST_REQUEST);
            InitializeHeader(testRequest);
            testRequest.SetField(new Fields.TestReqID(id));
            return SendRaw(testRequest, 0);
        }

        /// <summary>
        /// Send a basic Logout message
        /// </summary>
        /// <returns></returns>
        private bool GenerateLogout()
        {
            return GenerateLogout(null, null);
        }

        /// <summary>
        /// Send a Logout message
        /// </summary>
        /// <param name="text">written into the Text field</param>
        /// <returns></returns>
        private bool GenerateLogout(string text)
        {
            return GenerateLogout(null, text);
        }

        /// <summary>
        /// Send a Logout message
        /// </summary>
        /// <param name="other">used to fill MsgSeqNum field, if configuration requires it</param>
        /// <returns></returns>
        private bool GenerateLogout(Message other)
        {
            return GenerateLogout(other, null);
        }

        /// <summary>
        /// Send a Logout message
        /// </summary>
        /// <param name="other">used to fill MsgSeqNum field, if configuration requires it; ignored if null</param>
        /// <param name="text">written into the Text field; ignored if empty/null</param>
        /// <returns></returns>
        private bool GenerateLogout(Message other, string text)
        {
            var logoutMessage = msgFactory_.Create(this.SessionID.BeginString, Fields.MsgType.LOGOUT);
            InitializeHeader(logoutMessage);
            if (!string.IsNullOrEmpty(text))
                logoutMessage.SetField(new Fields.Text(text));
            if (other != null && this.EnableLastMsgSeqNumProcessed)
            {
                try
                {
                    logoutMessage.Header.SetField(new Fields.LastMsgSeqNumProcessed(other.Header.GetInt(Tags.MsgSeqNum)));
                }
                catch (FieldNotFoundException)
                {
                    this.Log.OnEvent("Error: No message sequence number: " + other);
                }
            }
            state_.SentLogout = SendRaw(logoutMessage, 0);
            return state_.SentLogout;
        }

        public bool GenerateHeartbeat()
        {
            Message heartbeat = msgFactory_.Create(this.SessionID.BeginString, Fields.MsgType.HEARTBEAT);
            InitializeHeader(heartbeat);
            return SendRaw(heartbeat, 0);
        }
        
       

        private bool GenerateReject(MessageBuilder msgBuilder, FixValues.SessionRejectReason reason)
        {
            return GenerateReject(msgBuilder.RejectableMessage(), reason, 0);
        }

        private bool GenerateReject(MessageBuilder msgBuilder, FixValues.SessionRejectReason reason, int field)
        {
            return GenerateReject(msgBuilder.RejectableMessage(), reason, field);
        }

        private bool GenerateReject(Message message, FixValues.SessionRejectReason reason)
        {
            return GenerateReject(message, reason, 0);
        }

        private bool GenerateReject(Message message, FixValues.SessionRejectReason reason, int field)
        {
            string beginString = this.SessionID.BeginString;

            Message reject = msgFactory_.Create(beginString, Fields.MsgType.REJECT);
            reject.ReverseRoute(message.Header);
            InitializeHeader(reject);

            string msgType;
            if (message.Header.IsSetField(Fields.Tags.MsgType))
                msgType = message.Header.GetString(Fields.Tags.MsgType);
            else
                msgType = "";

            int msgSeqNum = 0;
            if (message.Header.IsSetField(Fields.Tags.MsgSeqNum))
            {
                try
                {
                    msgSeqNum = message.Header.GetInt(Fields.Tags.MsgSeqNum);
                    reject.SetField(new Fields.RefSeqNum(msgSeqNum));
                }
                catch (System.Exception)
                { }
            }

            if (beginString.CompareTo(FixValues.BeginString.FIX42) >= 0)
            {
                if (msgType.Length > 0)
                    reject.SetField(new Fields.RefMsgType(msgType));
                if ((FixValues.BeginString.FIX42.Equals(beginString) && reason.Value <= FixValues.SessionRejectReason.INVALID_MSGTYPE.Value) || (beginString.CompareTo(FixValues.BeginString.FIX42) > 0))
                {
                    reject.SetField(new Fields.SessionRejectReason(reason.Value));
                }
            }
            if (!MsgType.LOGON.Equals(msgType)
              && !MsgType.SEQUENCE_RESET.Equals(msgType)
              && (msgSeqNum == state_.GetNextTargetMsgSeqNum()))
            {
                state_.IncrNextTargetMsgSeqNum();
            }

            if ((0 != field) || FixValues.SessionRejectReason.INVALID_TAG_NUMBER.Equals(reason))
            {
                if (FixValues.SessionRejectReason.INVALID_MSGTYPE.Equals(reason))
                {
                    if (this.SessionID.BeginString.CompareTo(FixValues.BeginString.FIX43) >= 0)
                        PopulateRejectReason(reject, reason.Description);
                    else
                        PopulateSessionRejectReason(reject, field, reason.Description, false);
                }
                else
                    PopulateSessionRejectReason(reject, field, reason.Description, true);

                this.Log.OnEvent("Message " + msgSeqNum + " Rejected: " + reason.Description + " (Field=" + field + ")");
            }
            else
            {
                PopulateRejectReason(reject, reason.Description);
                this.Log.OnEvent("Message " + msgSeqNum + " Rejected: " + reason.Value);
            }

            if (!state_.ReceivedLogon)
                throw new QuickFIXException("Tried to send a reject while not logged on");

            return SendRaw(reject, 0);
        }

        private void PopulateSessionRejectReason(Message reject, int field, string text, bool includeFieldInfo)
        {
            if (this.SessionID.BeginString.CompareTo(FixValues.BeginString.FIX42) >= 0)
            {
                reject.SetField(new Fields.RefTagID(field));
                reject.SetField(new Fields.Text(text));
            }
            else
            {
                if (includeFieldInfo)
                    reject.SetField(new Fields.Text(text + " (" + field + ")"));
                else
                    reject.SetField(new Fields.Text(text));
            }
        }

        private void PopulateRejectReason(Message reject, string text)
        {
            reject.SetField(new Fields.Text(text));
        }

        /// <summary>
        /// FIXME don't do so much operator new here
        /// </summary>
        /// <param name="m"></param>
        /// <param name="msgSeqNum"></param>
        private void InitializeHeader(Message m, int msgSeqNum)
        {
            state_.LastSentTimeDT = DateTime.UtcNow;
            m.Header.SetField(new Fields.BeginString(this.SessionID.BeginString));
            m.Header.SetField(new Fields.SenderCompID(this.SessionID.SenderCompID));
            if (SessionID.IsSet(this.SessionID.SenderSubID))
                m.Header.SetField(new Fields.SenderSubID(this.SessionID.SenderSubID));
            if (SessionID.IsSet(this.SessionID.SenderLocationID))
                m.Header.SetField(new Fields.SenderLocationID(this.SessionID.SenderLocationID));
            m.Header.SetField(new Fields.TargetCompID(this.SessionID.TargetCompID));
            if (SessionID.IsSet(this.SessionID.TargetSubID))
                m.Header.SetField(new Fields.TargetSubID(this.SessionID.TargetSubID));
            if (SessionID.IsSet(this.SessionID.TargetLocationID))
                m.Header.SetField(new Fields.TargetLocationID(this.SessionID.TargetLocationID));

            if (msgSeqNum > 0)
                m.Header.SetField(new Fields.MsgSeqNum(msgSeqNum));
            else
                m.Header.SetField(new Fields.MsgSeqNum(state_.GetNextSenderMsgSeqNum()));

            if (this.EnableLastMsgSeqNumProcessed && !m.Header.IsSetField(Tags.LastMsgSeqNumProcessed))
            {
                m.Header.SetField(new LastMsgSeqNumProcessed(this.NextTargetMsgSeqNum - 1));
            }

            InsertSendingTime(m.Header);
        }

        private void InitializeHeader(Message m)
        {
            InitializeHeader(m, 0);
        }

        private void InsertSendingTime(FieldMap header)
        {
            bool fix42OrAbove = false;
            if (this.SessionID.BeginString == FixValues.BeginString.FIXT11)
                fix42OrAbove = true;
            else
                fix42OrAbove = this.SessionID.BeginString.CompareTo(FixValues.BeginString.FIX42) >= 0;

            header.SetField(new Fields.SendingTime(System.DateTime.UtcNow, fix42OrAbove ? TimeStampPrecision : TimeStampPrecision.Second ) );
        }

        private void Persist(Message message, string messageString)
        {
            if (this.PersistMessages)
            {
                int msgSeqNum = message.Header.GetInt(Fields.Tags.MsgSeqNum);
                state_.Set(msgSeqNum, messageString);
            }
            state_.IncrNextSenderMsgSeqNum();
        }

        private bool IsGoodTime(Message msg)
        {
            if (!CheckLatency)
                return true;

            var sendingTime = msg.Header.GetDateTime(Fields.Tags.SendingTime);
            System.TimeSpan tmSpan = System.DateTime.UtcNow - sendingTime;
            if (System.Math.Abs(tmSpan.TotalSeconds) > MaxLatency)
            {
                return false;
            }
            return true;
        }

        private void GenerateSequenceReset(Message receivedMessage, int beginSeqNo, int endSeqNo)
        {
            string beginString = this.SessionID.BeginString;
            Message sequenceReset = msgFactory_.Create(beginString, Fields.MsgType.SEQUENCE_RESET);
            InitializeHeader(sequenceReset);
            int newSeqNo = endSeqNo;
            sequenceReset.Header.SetField(new PossDupFlag(true));
            InsertOrigSendingTime(sequenceReset.Header, sequenceReset.Header.GetDateTime(Tags.SendingTime));

            sequenceReset.Header.SetField(new MsgSeqNum(beginSeqNo));
            sequenceReset.SetField(new NewSeqNo(newSeqNo));
            sequenceReset.SetField(new GapFillFlag(true));
            if (receivedMessage != null && this.EnableLastMsgSeqNumProcessed)
            {
                try
                {
                    sequenceReset.Header.SetField(new Fields.LastMsgSeqNumProcessed(receivedMessage.Header.GetInt(Tags.MsgSeqNum)));
                }
                catch (FieldNotFoundException)
                {
                    this.Log.OnEvent("Error: Received message without MsgSeqNum: " + receivedMessage);
                }
            }
            SendRaw(sequenceReset, beginSeqNo);
            this.Log.OnEvent("Sent SequenceReset TO: " + newSeqNo);
        }

        private void InsertOrigSendingTime(FieldMap header, System.DateTime sendingTime)
        {
            bool fix42OrAbove = false;
            if (this.SessionID.BeginString == FixValues.BeginString.FIXT11)
                fix42OrAbove = true;
            else
                fix42OrAbove = this.SessionID.BeginString.CompareTo(FixValues.BeginString.FIX42) >= 0;

            header.SetField(new OrigSendingTime(sendingTime, fix42OrAbove ? TimeStampPrecision : TimeStampPrecision.Second ) );
        }

        private async Task NextQueued(CancellationToken cancellationToken)
        {
            while ( await NextQueued(state_.MessageStore.GetNextTargetMsgSeqNum(), cancellationToken))
            {
                // continue
            }
        }

        private async Task<bool> NextQueued(int num, CancellationToken cancellationToken)
        {
            Message msg = state_.Dequeue(num);

            if (msg != null)
            {
                Log.OnEvent("Processing queued message: " + num);

                string msgType = msg.Header.GetString(Tags.MsgType);
                if (msgType.Equals(MsgType.LOGON) || msgType.Equals(MsgType.RESEND_REQUEST))
                {
                    state_.IncrNextTargetMsgSeqNum();
                }
                else
                {
                    await NextMessage(msg.ToString(), cancellationToken);
                }
                return true;
            }
            return false;
        }

        private bool IsAdminMessage(Message msg)
        {
            var msgType = msg.Header.GetString(Fields.Tags.MsgType);
            return AdminMsgTypes.Contains(msgType);
        }

        private bool SendRaw(Message message, int seqNum)
        {
            lock (sync_)
            {
                string msgType = message.Header.GetString(Fields.Tags.MsgType);

                InitializeHeader(message, seqNum);

                if (Message.IsAdminMsgType(msgType))
                {
                    this.Application.ToAdmin(message, this.SessionID);

                    if (MsgType.LOGON.Equals(msgType) && !state_.ReceivedReset)
                    {
                        Fields.ResetSeqNumFlag resetSeqNumFlag = new QuickFix.Fields.ResetSeqNumFlag(false);
                        if (message.IsSetField(resetSeqNumFlag))
                            message.GetField(resetSeqNumFlag);
                        if (resetSeqNumFlag.getValue())
                        {
                            state_.Reset("ResetSeqNumFlag");
                            message.Header.SetField(new Fields.MsgSeqNum(state_.GetNextSenderMsgSeqNum()));
                        }
                        state_.SentReset = resetSeqNumFlag.Obj;
                    }
                }
                else
                {
                    try
                    {
                        this.Application.ToApp(message, this.SessionID);
                    }
                    catch (DoNotSend)
                    {
                        return false;
                    }
                }

                string messageString = message.ToString();
                if (0 == seqNum)
                    Persist(message, messageString);
                return Send(messageString);
            }
        }

        public void Dispose()
        {
            if (!disposed_)
            {
                state_?.Dispose();
                sessions_.TryRemove(this.SessionID, out _);
                disposed_ = true;
            }
            _awaitableCriticalSection.Dispose();
        }

        public bool Disposed => disposed_;
    }
}
