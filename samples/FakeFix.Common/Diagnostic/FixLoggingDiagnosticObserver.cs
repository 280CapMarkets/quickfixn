using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Extensions.DiagnosticAdapter;
using QuickFix;

namespace FakeFix.Common.Diagnostic
{
    public sealed class FixLoggingDiagnosticObserver : IObserver<DiagnosticListener>
    {
        private readonly List<IDisposable> _subscriptions = new List<IDisposable>();
        public void OnCompleted()
        {
            _subscriptions.ForEach(s => s.Dispose());
            _subscriptions.Clear();
        }

        public void OnError(Exception error)
        {

        }

        public void OnNext(DiagnosticListener diagnosticListener)
        {
            if (diagnosticListener.Name != FixApp.ListenerName) return;

            var subscription = diagnosticListener.SubscribeWithAdapter(this);
            _subscriptions.Add(subscription);
        }

        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.ToAdmin))]
        public void ToAdmin(Message message, SessionID sessionId) => Console.WriteLine($"To admin message [{message}] in session [{sessionId}]");

        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.FromAdmin))]
        public void FromAdmin(Message message, SessionID sessionId) => Console.WriteLine($"From admin message [{message}] in session [{sessionId}]");

        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.ToApp))]
        public void ToApp(Message message, SessionID sessionId) => Console.WriteLine($"To app message [{message}] in session [{sessionId}]");

        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.FromApp))]
        public void FromApp(Message message, SessionID sessionId) => Console.WriteLine($"From app message [{message}] in session [{sessionId}]");

        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.OnLogon))]
        public void OnLogon(SessionID sessionId) => Console.WriteLine($"Logon for session session : [{sessionId}]");

        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.OnLogout))]
        public void OnLogout(SessionID sessionId) => Console.WriteLine($"Log out for session session : [{sessionId}]");


        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.OnCreate))]
        public void OnCreate(SessionID sessionId) => Console.WriteLine($"Created session : [{sessionId}]");
    }
}
