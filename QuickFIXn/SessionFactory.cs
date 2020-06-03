using System.Collections.Generic;
using System.Threading;
using QuickFix.Fields.Converters;
using QuickFix.Session;

namespace QuickFix
{
    /// <summary>
    /// Creates a Session based on specified settings
    /// </summary>
    public class SessionFactory
    {
        private readonly IApplication application_;
        private readonly IMessageStoreFactory messageStoreFactory_;
        private readonly ILogFactory logFactory_;
        private readonly IMessageFactory messageFactory_;
        private readonly Dictionary<string,DataDictionary.DataDictionary> dictionariesByPath_ = new Dictionary<string,DataDictionary.DataDictionary>();

        public SessionFactory(IApplication app, IMessageStoreFactory storeFactory)
            : this(app, storeFactory, null, null)
        { }

        public SessionFactory(IApplication app, IMessageStoreFactory storeFactory, ILogFactory logFactory)
            : this(app, storeFactory, logFactory, null)
        { }

        public SessionFactory(IApplication app, IMessageStoreFactory storeFactory, ILogFactory logFactory, IMessageFactory messageFactory)
        {
            // TODO: for V2, consider ONLY instantiating MessageFactory in the Create() method,
            //   and removing instance var messageFactory_ altogether.
            //   This makes sense because we can't distinguish FIX50 versions here in this constructor,
            //   and thus can't create the right FIX-Version factory because we don't know what
            //   session to use to look up the BeginString and DefaultApplVerID.

            application_ = app;
            messageStoreFactory_ = storeFactory;
            logFactory_ = logFactory ?? new NullLogFactory();
            messageFactory_ = messageFactory ?? new DefaultMessageFactory();

            System.Console.WriteLine("[SessionFactory] " + messageFactory_.GetType().FullName);
        }

        public Session.Session Create(SessionID sessionID, QuickFix.Dictionary settings, CancellationToken cancellationToken)
        {
            string connectionType = settings.GetString(SessionSettings.CONNECTION_TYPE);
            if (!"acceptor".Equals(connectionType) && !"initiator".Equals(connectionType))
                throw new ConfigError("Invalid ConnectionType");

            if ("acceptor".Equals(connectionType) && settings.Has(SessionSettings.SESSION_QUALIFIER))
                throw new ConfigError("SessionQualifier cannot be used with acceptor.");

            bool useDataDictionary = true;
            if (settings.Has(SessionSettings.USE_DATA_DICTIONARY))
                useDataDictionary = settings.GetBool(SessionSettings.USE_DATA_DICTIONARY);

            QuickFix.Fields.ApplVerID defaultApplVerID = null;
            IMessageFactory sessionMsgFactory = messageFactory_;
            if (sessionID.IsFIXT)
            {
                if (!settings.Has(SessionSettings.DEFAULT_APPLVERID))
                {
                    throw new ConfigError("ApplVerID is required for FIXT transport");
                }
                string rawDefaultApplVerIdSetting = settings.GetString(SessionSettings.DEFAULT_APPLVERID);

                defaultApplVerID = Message.GetApplVerID(rawDefaultApplVerIdSetting);

                // DefaultMessageFactory as created in the SessionFactory ctor cannot
                // tell the difference between FIX50 versions (same BeginString, unknown defaultApplVerId).
                // But we have the real session settings here, so we can fix that.
                // This is, of course, kind of a hack, and it should be reworked in V2 (TODO!).
                if (messageFactory_ is DefaultMessageFactory)
                {
                    sessionMsgFactory = new DefaultMessageFactory(
                        FixValues.ApplVerID.FromBeginString(rawDefaultApplVerIdSetting));
                }
            }

            DataDictionaryProvider dd = new DataDictionaryProvider();
            if (useDataDictionary)
            {
                if (sessionID.IsFIXT)
                    ProcessFixTDataDictionaries(sessionID, settings, dd);
                else
                    ProcessFixDataDictionary(sessionID, settings, dd);
            }
            
            string senderDefaultApplVerId = "";
            if(defaultApplVerID != null)
                senderDefaultApplVerId = defaultApplVerID.Obj;

            var sessionConfiguration = CreateSessionConfiguration(settings, connectionType);

            var session = new Session.Session(
                application_,
                messageStoreFactory_,
                sessionID,
                dd,
                new SessionSchedule(settings),
                sessionConfiguration,
                logFactory_,
                sessionMsgFactory,
                senderDefaultApplVerId,
                cancellationToken);

            return session;
        }

        private SessionConfiguration CreateSessionConfiguration(QuickFix.Dictionary settings, string connectionType)
        {
            var heartBtInt = 0;
            if (connectionType == "initiator")
            {
                heartBtInt = System.Convert.ToInt32(settings.GetLong(SessionSettings.HEARTBTINT));
                if (heartBtInt <= 0)
                    throw new ConfigError("Heartbeat must be greater than zero");
            }

            /* FIXME - implement optional settings
            if (settings.Has(SessionSettings.CHECK_COMPID))
                session.SetCheckCompId(settings.GetBool(SessionSettings.CHECK_COMPID));
             */
            var sessionConfiguration = new SessionConfiguration
            {
                HeartBtInt = heartBtInt,
                CheckCompId = true,
                CheckLatency = !settings.Has(SessionSettings.CHECK_LATENCY) || settings.GetBool(SessionSettings.CHECK_LATENCY),
                MaxLatency = settings.Has(SessionSettings.MAX_LATENCY) ? settings.GetInt(SessionSettings.MAX_LATENCY) : 120,
                LogonTimeout = settings.Has(SessionSettings.LOGON_TIMEOUT) ? settings.GetInt(SessionSettings.LOGON_TIMEOUT) : default,
                LogoutTimeout = settings.Has(SessionSettings.LOGOUT_TIMEOUT) ? settings.GetInt(SessionSettings.LOGOUT_TIMEOUT) : default,
                ResetOnLogon = settings.Has(SessionSettings.RESET_ON_LOGON) ? settings.GetBool(SessionSettings.RESET_ON_LOGON) : default,
                ResetOnLogout = settings.Has(SessionSettings.RESET_ON_LOGOUT) ? settings.GetBool(SessionSettings.RESET_ON_LOGOUT) : default,
                ResetOnDisconnect = settings.Has(SessionSettings.RESET_ON_DISCONNECT) && settings.GetBool(SessionSettings.RESET_ON_DISCONNECT),
                RefreshOnLogon = settings.Has(SessionSettings.REFRESH_ON_LOGON) ? settings.GetBool(SessionSettings.REFRESH_ON_LOGON) : default,
                PersistMessages = !settings.Has(SessionSettings.PERSIST_MESSAGES) || settings.GetBool(SessionSettings.PERSIST_MESSAGES),
                MillisecondsInTimeStamp = settings.Has(SessionSettings.MILLISECONDS_IN_TIMESTAMP) ? settings.GetBool(SessionSettings.MILLISECONDS_IN_TIMESTAMP) : default,
                TimeStampPrecision = settings.Has(SessionSettings.TIMESTAMP_PRECISION) ? settings.GetTimeStampPrecision(SessionSettings.TIMESTAMP_PRECISION) : TimeStampPrecision.Millisecond,
                EnableLastMsgSeqNumProcessed = settings.Has(SessionSettings.ENABLE_LAST_MSG_SEQ_NUM_PROCESSED) && settings.GetBool(SessionSettings.ENABLE_LAST_MSG_SEQ_NUM_PROCESSED),
                MaxMessagesInResendRequest = settings.Has(SessionSettings.MAX_MESSAGES_IN_RESEND_REQUEST) ? settings.GetInt(SessionSettings.MAX_MESSAGES_IN_RESEND_REQUEST) : 0,
                SendLogoutBeforeTimeoutDisconnect = settings.Has(SessionSettings.SEND_LOGOUT_BEFORE_TIMEOUT_DISCONNECT) && settings.GetBool(SessionSettings.SEND_LOGOUT_BEFORE_TIMEOUT_DISCONNECT),
                IgnorePossDupResendRequests = settings.Has(SessionSettings.IGNORE_POSSDUP_RESEND_REQUESTS) && settings.GetBool(SessionSettings.IGNORE_POSSDUP_RESEND_REQUESTS),
                ValidateLengthAndChecksum = !settings.Has(SessionSettings.VALIDATE_LENGTH_AND_CHECKSUM) || settings.GetBool(SessionSettings.VALIDATE_LENGTH_AND_CHECKSUM),
                RequiresOrigSendingTime = !settings.Has(SessionSettings.RESETSEQUENCE_MESSAGE_REQUIRES_ORIGSENDINGTIME) || settings.GetBool(SessionSettings.RESETSEQUENCE_MESSAGE_REQUIRES_ORIGSENDINGTIME),
                SendRedundantResendRequests = settings.Has(SessionSettings.SEND_REDUNDANT_RESENDREQUESTS) && settings.GetBool(SessionSettings.SEND_REDUNDANT_RESENDREQUESTS),
                ResendSessionLevelRejects = settings.Has(SessionSettings.RESEND_SESSION_LEVEL_REJECTS) && settings.GetBool(SessionSettings.RESEND_SESSION_LEVEL_REJECTS),
            };
            return sessionConfiguration;
        }

        private DataDictionary.DataDictionary createDataDictionary(SessionID sessionID, QuickFix.Dictionary settings, string settingsKey, string beginString)
        {
            DataDictionary.DataDictionary dd;
            string path;
            if (settings.Has(settingsKey))
                path = settings.GetString(settingsKey);
            else
                path = beginString.Replace(".", "") + ".xml";

            if (!dictionariesByPath_.TryGetValue(path, out dd))
            {
                dd = new DataDictionary.DataDictionary(path);
                dictionariesByPath_[path] = dd;
            }

            DataDictionary.DataDictionary ddCopy = new DataDictionary.DataDictionary(dd);

            if (settings.Has(SessionSettings.VALIDATE_FIELDS_OUT_OF_ORDER))
                ddCopy.CheckFieldsOutOfOrder = settings.GetBool(SessionSettings.VALIDATE_FIELDS_OUT_OF_ORDER);
            if (settings.Has(SessionSettings.VALIDATE_FIELDS_HAVE_VALUES))
                ddCopy.CheckFieldsHaveValues = settings.GetBool(SessionSettings.VALIDATE_FIELDS_HAVE_VALUES);
            if (settings.Has(SessionSettings.VALIDATE_USER_DEFINED_FIELDS))
                ddCopy.CheckUserDefinedFields = settings.GetBool(SessionSettings.VALIDATE_USER_DEFINED_FIELDS);
            if (settings.Has(SessionSettings.ALLOW_UNKNOWN_MSG_FIELDS))
                ddCopy.AllowUnknownMessageFields = settings.GetBool(SessionSettings.ALLOW_UNKNOWN_MSG_FIELDS);

            return ddCopy;
        }

        private void ProcessFixTDataDictionaries(SessionID sessionID, Dictionary settings, DataDictionaryProvider provider)
        {
            provider.AddTransportDataDictionary(sessionID.BeginString, createDataDictionary(sessionID, settings, SessionSettings.TRANSPORT_DATA_DICTIONARY, sessionID.BeginString));
    
            foreach (KeyValuePair<string, string> setting in settings)
            {
                if (setting.Key.StartsWith(SessionSettings.APP_DATA_DICTIONARY, System.StringComparison.InvariantCultureIgnoreCase))
                {
                    if (setting.Key.Equals(SessionSettings.APP_DATA_DICTIONARY, System.StringComparison.InvariantCultureIgnoreCase))
                    {
                        Fields.ApplVerID applVerId = Message.GetApplVerID(settings.GetString(SessionSettings.DEFAULT_APPLVERID));
                        DataDictionary.DataDictionary dd = createDataDictionary(sessionID, settings, SessionSettings.APP_DATA_DICTIONARY, sessionID.BeginString);
                        provider.AddApplicationDataDictionary(applVerId.Obj, dd);
                    }
                    else
                    {
                        int offset = setting.Key.IndexOf(".");
                        if (offset == -1)
                            throw new System.ArgumentException(string.Format("Malformed {0} : {1}", SessionSettings.APP_DATA_DICTIONARY, setting.Key));

                        string beginStringQualifier = setting.Key.Substring(offset);
                        DataDictionary.DataDictionary dd = createDataDictionary(sessionID, settings, setting.Key, beginStringQualifier);
                        provider.AddApplicationDataDictionary(Message.GetApplVerID(beginStringQualifier).Obj, dd);
                    }
                }
            }
        }

        private void ProcessFixDataDictionary(SessionID sessionID, Dictionary settings, DataDictionaryProvider provider)
        {
            DataDictionary.DataDictionary dataDictionary = createDataDictionary(sessionID, settings, SessionSettings.DATA_DICTIONARY, sessionID.BeginString);
            provider.AddTransportDataDictionary(sessionID.BeginString, dataDictionary);
            provider.AddApplicationDataDictionary(FixValues.ApplVerID.FromBeginString(sessionID.BeginString), dataDictionary);
        }

    }
}
