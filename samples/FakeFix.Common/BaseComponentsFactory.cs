using System;
using System.Diagnostics;
using System.Threading;
using FakeFix.Common.Diagnostic;
using FakeFix.Common.Interfaces;
using QuickFix;
using QuickFix.Transport;

namespace FakeFix.Common
{
    public class BaseComponentsFactory
    {
        public IDisposable CreateDiagnosticSubscription(IObserver<DiagnosticListener> observer = default) =>
            DiagnosticListener.AllListeners.Subscribe(observer ?? new FixLoggingDiagnosticObserver());

        public IFixApp CreateApp(CancellationTokenSource cancellationTokenSource, Func<string, Message> messageFactoryFunc = default) => new FixApp(cancellationTokenSource, messageFactoryFunc);

        public IInitiator CreateFixClient(IFixApp app, string configFilePath) => new SocketInitiator(app, new FixStoreFactory(), new SessionSettings(configFilePath), new FixLogFactory());
        public IAcceptor CreateFixServer(IFixApp app, string configFilePath) => new ThreadedSocketAcceptor(app, new FixStoreFactory(), new SessionSettings(configFilePath), new FixLogFactory());
    }
}
