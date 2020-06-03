using System;
using System.Collections.Generic;
using System.Text;
using QuickFix.Fields.Converters;

namespace QuickFix.Session
{
    public class SessionConfiguration
    {
        public int HeartBtInt { get; set; }
        public bool? SendRedundantResendRequests { get; set; }
        public bool? ResendSessionLevelRejects { get; set; }
        public bool? CheckLatency { get; set; }
        public int? MaxLatency { get; set; }
        public int? LogonTimeout { get; set; }
        public int? LogoutTimeout { get; set; }
        public bool? ResetOnLogon { get; set; }
        public bool? ResetOnLogout { get; set; }
        public bool? ResetOnDisconnect { get; set; }
        public bool? RefreshOnLogon { get; set; }
        public bool? PersistMessages { get; set; }
        public bool? MillisecondsInTimeStamp { get; set; }
        public TimeStampPrecision? TimeStampPrecision { get; set; }
        public bool? EnableLastMsgSeqNumProcessed { get; set; }
        public int? MaxMessagesInResendRequest { get; set; }
        public bool? SendLogoutBeforeTimeoutDisconnect { get; set; }
        public bool? IgnorePossDupResendRequests { get; set; }
        public bool? ValidateLengthAndChecksum { get; set; }
        public bool? RequiresOrigSendingTime { get; set; }
        public bool? CheckCompId { get; set; }
    }
}
