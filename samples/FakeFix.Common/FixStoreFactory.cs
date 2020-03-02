using QuickFix;

namespace FakeFix.Common
{
    internal class FixStoreFactory : IMessageStoreFactory
    {
        public IMessageStore Create(SessionID sessionId) => new FixMessageStore();
    }
}
