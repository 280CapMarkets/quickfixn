using System;
using System.Collections.Generic;
using System.Threading;
using QuickFix.StateManagement;

namespace QuickFix
{
    // v2 TODO - consider making this internal
    /// <summary>
    /// Used by the session communications code. Not intended to be used by applications.
    /// </summary>
    public class SessionState : IDisposable, IConnectionState
    {
        #region Private Members

       
        private long _connectionState =  (long)ConnectionState.Disconnected;
        private int _heartBtInt;
        private int _logonTimeout;
        private int _logoutTimeout;
        private ResendRange _resendRange = new ResendRange();

        #endregion

        #region Unsynchronized Properties

        public IMessageStore MessageStore { get; set; }
        public bool IsInitiator { get; }

        public bool ShouldSendLogon => IsInitiator && !SentLogon;

        public ILog Log { get; }

        #endregion


        #region implementation of IConnectionState

        public bool IsDisconnected => IsInState(ConnectionState.Disconnected);
        public bool IsConnected => IsInState(ConnectionState.Connected);
        public bool CanDisconnect => IsInState(ConnectionState.Connected | ConnectionState.Pending);
        public void SetPending() => ConnectionState = ConnectionState.Pending;
        public void SetDisconnected() => ConnectionState = ConnectionState.Disconnected;
        public void SetConnected() => ConnectionState = ConnectionState.Connected;

        #endregion


        #region Synchronized Properties

        public ConnectionState ConnectionState
        {
            get => (ConnectionState)Interlocked.Read(ref _connectionState);
            set => Interlocked.Exchange(ref _connectionState, (long) value);
        }

        private bool IsInState(ConnectionState state) => (ConnectionState & state) != 0;


        public bool IsEnabled { get; set; } = true;
        public bool ReceivedLogon { get; set; }
        public bool ReceivedReset { get; set; }
        public bool SentLogon { get; set; }
        public bool SentLogout { get; set; }
        public bool SentReset { get; set; }

        public string LogoutReason { get; set; } = string.Empty;

        public int TestRequestCounter { get; set; }

        public int HeartBtInt
        {
            get => _heartBtInt;
            set
            {
                _heartBtInt = value; 
                HeartBtIntAsMilliSecs = 1000 * value;
            }
        }

        private int HeartBtIntAsMilliSecs { get; set; }

        public DateTime LastReceivedTimeDT { get; set; }

        public DateTime LastSentTimeDT { get; set; }

        public int LogonTimeout
        {
            get => _logonTimeout;
            set
            {
                _logonTimeout = value;
                LogonTimeoutAsMilliSecs = 1000 * value;
            }
        } 

        private long LogonTimeoutAsMilliSecs { get; set; }

        public int LogoutTimeout
        {
            get => _logoutTimeout;
            set { _logoutTimeout = value; LogoutTimeoutAsMilliSecs = 1000 * value; }
        }

        private long LogoutTimeoutAsMilliSecs { get; set; }

        private Dictionary<int, Message> MsgQueue { get; } = new Dictionary<int, Message>();

        #endregion

        public SessionState(ILog log, int heartBtInt)
        {
            Log = log;
            HeartBtInt = heartBtInt;
            IsInitiator = (0 != heartBtInt);
            LogonTimeout = 10;
            LogoutTimeout = 2;
            LastSentTimeDT =  LastReceivedTimeDT = DateTime.UtcNow;
        }

        /// <summary>
        /// All time args are in milliseconds
        /// </summary>
        /// <param name="now">current system time</param>
        /// <param name="lastReceivedTime">last received time</param>
        /// <param name="logonTimeout">number of milliseconds to wait for a Logon from the counterparty</param>
        /// <returns></returns>
        public static bool LogonTimedOut(DateTime now, long logonTimeout, DateTime lastReceivedTime) => (now.Subtract(lastReceivedTime).TotalMilliseconds) >= logonTimeout;

        public bool LogonTimedOut() => LogonTimedOut(DateTime.UtcNow, this.LogonTimeoutAsMilliSecs, this.LastReceivedTimeDT);

        /// <summary>
        /// All time args are in milliseconds
        /// </summary>
        /// <param name="now">current system datetime</param>
        /// <param name="heartBtIntMillis">heartbeat interval in milliseconds</param>
        /// <param name="lastReceivedTime">last received datetime</param>
        /// <returns>true if timed out</returns>
        public static bool TimedOut(DateTime now, int heartBtIntMillis, DateTime lastReceivedTime)
        {
            var elapsed = now.Subtract(lastReceivedTime).TotalMilliseconds;
            return elapsed >= (2.4 * heartBtIntMillis);
        }
        public bool TimedOut() => TimedOut(DateTime.UtcNow, this.HeartBtIntAsMilliSecs, this.LastReceivedTimeDT);

        /// <summary>
        /// All time args are in milliseconds
        /// </summary>
        /// <param name="now">current system time</param>
        /// <param name="sentLogout">true if a Logout has been sent to the counterparty, otherwise false</param>
        /// <param name="logoutTimeout">number of milliseconds to wait for a Logout from the counterparty</param>
        /// <param name="lastSentTime">last sent time</param>
        /// <returns></returns>
        public static bool LogoutTimedOut(DateTime now, bool sentLogout, long logoutTimeout, DateTime lastSentTime) => sentLogout && ((now.Subtract(lastSentTime).TotalMilliseconds) >= logoutTimeout);

        public bool LogoutTimedOut() => LogoutTimedOut(DateTime.UtcNow, this.SentLogout, this.LogoutTimeoutAsMilliSecs, this.LastSentTimeDT);

        /// <summary>
        /// All time args are in milliseconds
        /// </summary>
        /// <param name="now">current system time</param>
        /// <param name="heartBtIntMillis">heartbeat interval in milliseconds</param>
        /// <param name="lastReceivedTime">last received time</param>
        /// <param name="testRequestCounter">test request counter</param>
        /// <returns>true if test request is needed</returns>
        public static bool NeedTestRequest(DateTime now, int heartBtIntMillis, DateTime lastReceivedTime, int testRequestCounter)
        {
            var elapsedMilliseconds = now.Subtract(lastReceivedTime).TotalMilliseconds;
            return elapsedMilliseconds >= (1.2 * ((testRequestCounter + 1) * heartBtIntMillis));
        }
        public bool NeedTestRequest() => NeedTestRequest(DateTime.UtcNow, this.HeartBtIntAsMilliSecs, this.LastReceivedTimeDT, this.TestRequestCounter);

        /// <summary>
        /// All time args are in milliseconds
        /// </summary>
        /// <param name="now">current system time</param>
        /// <param name="heartBtIntMillis">heartbeat interval in milliseconds</param>
        /// <param name="lastSentTime">last sent time</param>
        /// <param name="testRequestCounter">test request counter</param>
        /// <returns>true if heartbeat is needed</returns>
        public static bool NeedHeartbeat(DateTime now, int heartBtIntMillis, DateTime lastSentTime, int testRequestCounter)
        {
            var elapsed = now.Subtract(lastSentTime).TotalMilliseconds;
            return (elapsed >= Convert.ToDouble(heartBtIntMillis)) && (0 == testRequestCounter);
        }
        public bool NeedHeartbeat() => NeedHeartbeat(DateTime.UtcNow, this.HeartBtIntAsMilliSecs, this.LastSentTimeDT, this.TestRequestCounter);

        /// <summary>
        /// All time args are in milliseconds
        /// </summary>
        /// <param name="now">current system time</param>
        /// <param name="heartBtIntMillis">heartbeat interval in milliseconds</param>
        /// <param name="lastSentTime">last sent time</param>
        /// <param name="lastReceivedTime">last received time</param>
        /// <returns>true if within heartbeat interval</returns>
        public static bool WithinHeartbeat(DateTime now, int heartBtIntMillis, DateTime lastSentTime, DateTime lastReceivedTime) =>
            ((now.Subtract(lastSentTime).TotalMilliseconds) < Convert.ToDouble(heartBtIntMillis))
            && ((now.Subtract(lastReceivedTime).TotalMilliseconds) < Convert.ToDouble(heartBtIntMillis));

        public bool WithinHeartbeat() => WithinHeartbeat(DateTime.UtcNow, this.HeartBtIntAsMilliSecs, this.LastSentTimeDT, this.LastReceivedTimeDT);

        public ResendRange GetResendRange() => _resendRange;

        public void Get(int begSeqNo, int endSeqNo, List<string> messages) => MessageStore.Get(begSeqNo, endSeqNo, messages);

        public void SetResendRange(int begin, int end) => SetResendRange(begin, end, -1);

        public void SetResendRange(int begin, int end, int chunkEnd)
        {
            _resendRange.BeginSeqNo = begin;
            _resendRange.EndSeqNo = end;
            _resendRange.ChunkEndSeqNo = chunkEnd == -1 ? end : chunkEnd;
        }

        public bool ResendRequested() => !(_resendRange.BeginSeqNo == 0 && _resendRange.EndSeqNo == 0);

        public void Queue(int msgSeqNum, Message msg)
        {
            if (!MsgQueue.ContainsKey(msgSeqNum))
            {
                MsgQueue.Add(msgSeqNum, msg);
            }
        }

        public void ClearQueue()
        {
            MsgQueue.Clear();
        }

        public QuickFix.Message Dequeue(int num)
        {
            if (MsgQueue.ContainsKey(num))
            {
                QuickFix.Message msg = MsgQueue[num];
                MsgQueue.Remove(num);
                return msg;
            }
            return null;
        }

        public Message Retrieve(int msgSeqNum)
        {
            Message msg = null;
            if (MsgQueue.ContainsKey(msgSeqNum))
            {
                msg = MsgQueue[msgSeqNum];
                MsgQueue.Remove(msgSeqNum);
            }
            return msg;
        }

        /// <summary>
        /// All time values are displayed in milliseconds.
        /// </summary>
        /// <returns>a string that represents the session state</returns>
        public override string ToString()
        {
            return new System.Text.StringBuilder("SessionState ")
                .Append("[ Now=").Append(DateTime.UtcNow)
                .Append(", HeartBtInt=").Append(this.HeartBtIntAsMilliSecs)
                .Append(", LastSentTime=").Append(this.LastSentTimeDT)
                .Append(", LastReceivedTime=").Append(this.LastReceivedTimeDT)
                .Append(", TestRequestCounter=").Append(this.TestRequestCounter)
                .Append(", WithinHeartbeat=").Append(WithinHeartbeat())
                .Append(", NeedHeartbeat=").Append(NeedHeartbeat())
                .Append(", NeedTestRequest=").Append(NeedTestRequest())
                .Append(", ResendRange=").Append(GetResendRange())
                .Append(" ]").ToString();

        }

        #region MessageStore-manipulating Members

        public bool Set(int msgSeqNum, string msg) => MessageStore.Set(msgSeqNum, msg);

        public int GetNextSenderMsgSeqNum() => MessageStore.GetNextSenderMsgSeqNum();

        public int GetNextTargetMsgSeqNum() => this.MessageStore.GetNextTargetMsgSeqNum();

        public void SetNextSenderMsgSeqNum(int value) => MessageStore.SetNextSenderMsgSeqNum(value);

        public void SetNextTargetMsgSeqNum(int value) => MessageStore.SetNextTargetMsgSeqNum(value);

        public void IncrNextSenderMsgSeqNum() => MessageStore.IncrNextSenderMsgSeqNum();

        public void IncrNextTargetMsgSeqNum() => MessageStore.IncrNextTargetMsgSeqNum();

        public System.DateTime? CreationTime => MessageStore.CreationTime;

        [Obsolete("Use Reset(reason) instead.")]
        public void Reset() => Reset("(unspecified reason)");

        public void Reset(string reason)
        {
            this.MessageStore.Reset();
            this.Log.OnEvent("Session reset: " + reason);
        }

        public void Refresh() => MessageStore.Refresh();

        #endregion

        public void Dispose()
        {
            Log?.Dispose();
            MessageStore?.Dispose();
        }
    }
}