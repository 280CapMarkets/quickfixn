using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace QuickFix
{
    /// <summary>
    /// Settings for sessions. Settings are grouped by FIX version and target ID
    /// There is also a default settings that is inherited by the session-specific sections.
    /// </summary>
    public class SessionSettings
    {
        #region Public Constants

        public const string BEGINSTRING = "BeginString";
        public const string SENDERCOMPID = "SenderCompID";
        public const string SENDERSUBID = "SenderSubID";
        public const string SENDERLOCID = "SenderLocationID";
        public const string TARGETCOMPID = "TargetCompID";
        public const string TARGETSUBID = "TargetSubID";
        public const string TARGETLOCID = "TargetLocationID";
        public const string SESSION_QUALIFIER = "SessionQualifier";
        public const string DEFAULT_APPLVERID = "DefaultApplVerID";
        public const string CONNECTION_TYPE = "ConnectionType";
        public const string USE_DATA_DICTIONARY = "UseDataDictionary";
        public const string NON_STOP_SESSION = "NonStopSession";
        public const string USE_LOCAL_TIME = "UseLocalTime";
        public const string USE_DATE_TIME = "UseDateTime";
        public const string TIME_ZONE = "TimeZone";
        public const string START_DAY = "StartDay";
        public const string END_DAY = "EndDay";
        public const string START_TIME = "StartTime";
        public const string END_TIME = "EndTime";
        public const string HEARTBTINT = "HeartBtInt";
        public const string SOCKET_ACCEPT_HOST = "SocketAcceptHost";
        public const string SOCKET_ACCEPT_PORT = "SocketAcceptPort";
        public const string SOCKET_CONNECT_HOST = "SocketConnectHost";
        public const string SOCKET_CONNECT_PORT = "SocketConnectPort";
        public const string RECONNECT_INTERVAL = "ReconnectInterval";
        public const string FILE_LOG_PATH = "FileLogPath";
        public const string DEBUG_FILE_LOG_PATH = "DebugFileLogPath";
        public const string FILE_STORE_PATH = "FileStorePath";
        public const string REFRESH_ON_LOGON = "RefreshOnLogon";
        public const string RESET_ON_LOGON = "ResetOnLogon";
        public const string RESET_ON_LOGOUT = "ResetOnLogout";
        public const string RESET_ON_DISCONNECT = "ResetOnDisconnect";
        public const string VALIDATE_FIELDS_OUT_OF_ORDER = "ValidateFieldsOutOfOrder";
        public const string VALIDATE_FIELDS_HAVE_VALUES = "ValidateFieldsHaveValues";
        public const string VALIDATE_USER_DEFINED_FIELDS = "ValidateUserDefinedFields";
        public const string VALIDATE_LENGTH_AND_CHECKSUM = "ValidateLengthAndChecksum";
        public const string DATA_DICTIONARY = "DataDictionary";
        public const string TRANSPORT_DATA_DICTIONARY = "TransportDataDictionary";
        public const string APP_DATA_DICTIONARY = "AppDataDictionary";
        public const string PERSIST_MESSAGES = "PersistMessages";
        public const string LOGON_TIMEOUT = "LogonTimeout";
        public const string LOGOUT_TIMEOUT = "LogoutTimeout";
        public const string SEND_REDUNDANT_RESENDREQUESTS = "SendRedundantResendRequests";
        public const string RESEND_SESSION_LEVEL_REJECTS = "ResendSessionLevelRejects";
        public const string MILLISECONDS_IN_TIMESTAMP = "MillisecondsInTimeStamp";
        public const string TIMESTAMP_PRECISION = "TimeStampPrecision";
        public const string ENABLE_LAST_MSG_SEQ_NUM_PROCESSED = "EnableLastMsgSeqNumProcessed";
        public const string MAX_MESSAGES_IN_RESEND_REQUEST = "MaxMessagesInResendRequest";
        public const string SEND_LOGOUT_BEFORE_TIMEOUT_DISCONNECT = "SendLogoutBeforeDisconnectFromTimeout";
        public const string SOCKET_NODELAY = "SocketNodelay";
        public const string SOCKET_SEND_BUFFER_SIZE = "SocketSendBufferSize";
        public const string SOCKET_RECEIVE_BUFFER_SIZE = "SocketReceiveBufferSize";
        public const string IGNORE_POSSDUP_RESEND_REQUESTS = "IgnorePossDupResendRequests";
        public const string RESETSEQUENCE_MESSAGE_REQUIRES_ORIGSENDINGTIME = "RequiresOrigSendingTime";
        public const string CHECK_LATENCY = "CheckLatency";
        public const string MAX_LATENCY = "MaxLatency";

        public const string SSL_ENABLE = "SSLEnable";
        public const string SSL_SERVERNAME = "SSLServerName";
        public const string SSL_PROTOCOLS = "SSLProtocols";
        public const string SSL_VALIDATE_CERTIFICATES = "SSLValidateCertificates";
        public const string SSL_CHECK_CERTIFICATE_REVOCATION = "SSLCheckCertificateRevocation";
        public const string SSL_CERTIFICATE = "SSLCertificate";
        public const string SSL_CERTIFICATE_PASSWORD = "SSLCertificatePassword";
        public const string SSL_REQUIRE_CLIENT_CERTIFICATE = "SSLRequireClientCertificate";
        public const string SSL_CA_CERTIFICATE = "SSLCACertificate";

        #endregion

        #region Private Members
        private readonly object _sync = new object();
        private QuickFix.Dictionary _defaults = new QuickFix.Dictionary();
        private readonly ConcurrentDictionary<SessionID, QuickFix.Dictionary> _settings = new ConcurrentDictionary<SessionID, Dictionary>();

        #endregion

        #region Constructors

        public SessionSettings(string file)
        {
            try
            {
                using var fs = File.Open(file, FileMode.Open, FileAccess.Read);
                using var sr = new StreamReader(fs);
                Load(sr);
            }
            catch (System.Exception e)
            {
                throw new ConfigError("File " + file + " not found (" + e.Message + ")");
            }
        }

        public SessionSettings(TextReader conf)
        {
            Load(conf);
        }

        public SessionSettings()
        { }

        #endregion
        public bool Has(SessionID sessionId) => _settings.ContainsKey(sessionId);

        /// <summary>
        /// Get global default settings
        /// </summary>
        /// <returns>Dictionary of settings from the [DEFAULT] section</returns>
        public QuickFix.Dictionary GetDefaultSettings()
        {
            return _defaults;
        }

        /// <summary>
        /// Get a dictionary for a session
        /// </summary>
        /// <param name="sessionId">the ID of the session</param>
        /// <returns>Dictionary of settings from the [SESSION] section for the given SessionID</returns>
        public Dictionary Get(SessionID sessionId)
        {
            if (!_settings.TryGetValue(sessionId, out var dict))
                throw new ConfigError("Session '" + sessionId + "' not found");
            return dict;
        }

        /// <summary>
        /// Remove existing session config from the settings
        /// </summary>
        /// <param name="sessionId">ID of session for which config is to be removed</param>
        /// <returns>true if removed, false if config for the session does not exist</returns>
        public bool Remove(SessionID sessionId) => _settings.TryRemove(sessionId, out _);

        /// <summary>
        /// Add new session config
        /// </summary>
        /// <param name="sessionId">ID of session for which to add config</param>
        /// <param name="settings">session config</param>
        public bool TrySet(SessionID sessionId, Dictionary settings)
        {
            lock (_sync)
            {
                if (Has(sessionId)) return false;
                Set(sessionId, settings);
                return true;
            }
        }

        public HashSet<SessionID> GetSessions() => new HashSet<SessionID>(_settings.Keys);

        public override string ToString()
        {
            var s = new System.Text.StringBuilder();
            s.AppendLine("[DEFAULT]");

            foreach (KeyValuePair<string, string> entry in _defaults)
                s.AppendFormat("{0}={1}{2}", entry.Key, entry.Value, Environment.NewLine);

            foreach (var entry in _settings)
            {
                s.AppendLine().AppendLine("[SESSION]");
                foreach (KeyValuePair<string, string> kvp in entry.Value)
                {
                    if (_defaults.Has(kvp.Key) && _defaults.GetString(kvp.Key).Equals(kvp.Value))
                        continue;

                    s.AppendFormat("{0}={1}{2}", kvp.Key, kvp.Value, Environment.NewLine);
                }
            }

            return s.ToString();
        }

        #region private methods

        private void Set(SessionID sessionID, QuickFix.Dictionary settings)
        {
            if (Has(sessionID))
                throw new ConfigError("Duplicate Session " + sessionID.ToString());
            settings.SetString(BEGINSTRING, sessionID.BeginString);
            settings.SetString(SENDERCOMPID, sessionID.SenderCompID);
            if (SessionID.IsSet(sessionID.SenderSubID))
                settings.SetString(SENDERSUBID, sessionID.SenderSubID);
            if (SessionID.IsSet(sessionID.SenderLocationID))
                settings.SetString(SENDERLOCID, sessionID.SenderLocationID);
            settings.SetString(TARGETCOMPID, sessionID.TargetCompID);
            if (SessionID.IsSet(sessionID.TargetSubID))
                settings.SetString(TARGETSUBID, sessionID.TargetSubID);
            if (SessionID.IsSet(sessionID.TargetLocationID))
                settings.SetString(TARGETLOCID, sessionID.TargetLocationID);
            settings.Merge(_defaults);
            Validate(settings);
            _settings[sessionID] = settings;
        }

        private void Validate(QuickFix.Dictionary dictionary)
        {
            var beginString = dictionary.GetString(BEGINSTRING);
            if (beginString != Values.BeginString_FIX40 &&
                beginString != Values.BeginString_FIX41 &&
                beginString != Values.BeginString_FIX42 &&
                beginString != Values.BeginString_FIX43 &&
                beginString != Values.BeginString_FIX44 &&
                beginString != Values.BeginString_FIXT11)
            {
                throw new ConfigError(BEGINSTRING + " (" + beginString + ") must be FIX.4.0 to FIX.4.4 or FIXT.1.1");
            }

            var connectionType = dictionary.GetString(CONNECTION_TYPE);
            if (!"initiator".Equals(connectionType) && !"acceptor".Equals(connectionType))
            {
                throw new ConfigError(CONNECTION_TYPE + " must be 'initiator' or 'acceptor'");
            }
        }

        private void SetDefaultSettingsForAllSessions(QuickFix.Dictionary defaults)
        {
            _defaults = defaults;
            foreach (var entry in _settings)
                entry.Value.Merge(_defaults);
        }

        private void Load(TextReader conf)
        {
            var settings = new Settings(conf);

            //---- load the DEFAULT section
            var section = settings.Get("DEFAULT");
            var def = new QuickFix.Dictionary();
            if (section.Count > 0)
                def = section.First.Value;
            SetDefaultSettingsForAllSessions(def);

            //---- load each SESSION section
            section = settings.Get("SESSION");
            foreach (QuickFix.Dictionary dict in section)
            {
                dict.Merge(def);

                var sessionQualifier = SessionID.NOT_SET;
                var senderSubID = SessionID.NOT_SET;
                var senderLocID = SessionID.NOT_SET;
                var targetSubID = SessionID.NOT_SET;
                var targetLocID = SessionID.NOT_SET;

                if (dict.Has(SESSION_QUALIFIER))
                    sessionQualifier = dict.GetString(SESSION_QUALIFIER);
                if (dict.Has(SENDERSUBID))
                    senderSubID = dict.GetString(SENDERSUBID);
                if (dict.Has(SENDERLOCID))
                    senderLocID = dict.GetString(SENDERLOCID);
                if (dict.Has(TARGETSUBID))
                    targetSubID = dict.GetString(TARGETSUBID);
                if (dict.Has(TARGETLOCID))
                    targetLocID = dict.GetString(TARGETLOCID);
                var sessionID = new SessionID(dict.GetString(BEGINSTRING), dict.GetString(SENDERCOMPID), senderSubID, senderLocID, dict.GetString(TARGETCOMPID), targetSubID, targetLocID, sessionQualifier);
                Set(sessionID, dict);
            }
        }

        #endregion
    }
}
