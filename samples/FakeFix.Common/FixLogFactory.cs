using QuickFix;

namespace FakeFix.Common
{
    internal class FixLogFactory : ILogFactory
    {
        public ILog Create(SessionID sessionId) => new NullLog();
    }
}
