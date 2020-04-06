using System;

namespace QuickFix
{
    [Flags]
    public enum ConnectionState : long
    {
        Pending = 1,
        LogOnInQueue = 2,
        LogOnSent = 4,
        LogOnReceived = 8,
        Connected = 2,
        Disconnected = 16
    }
}
