namespace QuickFix.Session
{
    public class SessionDetails
    {
        public bool IsInitiator { get; set; }
        public bool IsLoggedOn { get; set; }
        public bool IsEnabled { get; set; }
        /// <summary>
        /// Session setting for heartbeat interval (in seconds)
        /// </summary>
        public int HeartBtInt { get; set; }

        public int LogonTimeout { get; set; }

        //TODO: nmandzyk used from server code probably should be move to more lightweight structure
        public bool HasResponder { get; set; }

    }
}
