namespace QuickFix.StateManagement
{
    public interface IConnectionState
    {
        bool IsDisconnected { get; }
        bool IsConnected { get; }
        bool CanDisconnect { get; }

        void SetPending();
        void SetDisconnected();
        void SetConnected();
    }
}