using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FakeFix.Common.Interfaces;
using QuickFix;

namespace FakeFix.Common
{
    internal class FixApp : IFixApp
    {
        public const string ListenerName = "TwoEightyCap.FixCommon.FixApp";
        private static DiagnosticSource FixDiagnosticSource = new DiagnosticListener(ListenerName);
        
        private readonly Dictionary<string, Task> _sendingTasks;
        private readonly Func<string, Message> _messageFactoryFunc;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public FixApp(CancellationTokenSource cancellationTokenSource, Func<string, Message> messageFactoryFunc = default)
        {
            _cancellationTokenSource = cancellationTokenSource;
            _messageFactoryFunc = messageFactoryFunc;
            _sendingTasks = new Dictionary<string, Task>();
        }

        public void ToAdmin(Message message, SessionID sessionId) => WriteToSourceIfNeeded(nameof(ToAdmin), new { message, sessionId });

        public void FromAdmin(Message message, SessionID sessionId) => WriteToSourceIfNeeded(nameof(FromAdmin), new { message, sessionId });

        public void ToApp(Message message, SessionID sessionId) => WriteToSourceIfNeeded(nameof(ToApp), new { message, sessionId });

        public void FromApp(Message message, SessionID sessionId) => WriteToSourceIfNeeded(nameof(FromApp), new { message, sessionId });

        public void OnCreate(SessionID sessionId) => WriteToSourceIfNeeded(nameof(OnCreate), new { sessionId });

        public void OnLogout(SessionID sessionId)
        {
            WriteToSourceIfNeeded(nameof(OnLogout), new { sessionId });
            if (!_sendingTasks.TryGetValue(sessionId.ToString(), out var sendingTask)) return;

            sendingTask.Wait();
            _sendingTasks.Remove(sessionId.ToString());
            sendingTask?.Dispose();
        } 

        public void OnLogon(SessionID sessionId)
        {
            WriteToSourceIfNeeded(nameof(OnLogon), new { sessionId });
            if (_messageFactoryFunc == null || _sendingTasks.TryGetValue(sessionId.ToString(), out var sendingTask)) return;

            var session = Session.LookupSession(sessionId);
            sendingTask = Task.Factory.StartNew(() => SendingMessageWorker(session));
            _sendingTasks.Add(sessionId.ToString(), sendingTask);
        }

        public Task WhenStopped() => Task.WhenAll(_sendingTasks.Values.ToArray());
       
        private void WriteToSourceIfNeeded(string name, object value)
        {
            if (FixDiagnosticSource.IsEnabled($"{ListenerName}.{name}"))
                FixDiagnosticSource.Write($"{ListenerName}.{name}", value);
        }

        private void SendingMessageWorker(Session session)
        {
            var sessionId = session.SessionID.ToString();
            while (!_cancellationTokenSource.Token.IsCancellationRequested && session.IsLoggedOn)
            {
                session.Send(_messageFactoryFunc(sessionId));
            }
        }

        public void Dispose() => _sendingTasks.Values.ToList().ForEach(t => t?.Dispose());
    }
}
