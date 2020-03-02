using System;
using System.Collections.Generic;
using System.Diagnostics;
using FakeFix.Common.Diagnostic.Models;
using Microsoft.Extensions.DiagnosticAdapter;
using QuickFix;

namespace FakeFix.Common.Diagnostic
{
    public sealed class ReceiverDiagnosticObserver : IObserver<DiagnosticListener>
    {
        private readonly List<IDisposable> _subscriptions = new List<IDisposable>();
        private readonly Dictionary<SessionID, MetricData> _metricData;
        private readonly Random _random;

        public ReceiverDiagnosticObserver()
        {
            _metricData = new Dictionary<SessionID, MetricData>();
            _random = new Random();
        }


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
            if (diagnosticListener.Name != FixApp.ListenerName)
                return;

            var subscription = diagnosticListener.SubscribeWithAdapter(this);
            _subscriptions.Add(subscription);
        }

        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.FromAdmin))]
        public void FromAdmin(Message message, SessionID sessionId){
            if (_metricData.TryGetValue(sessionId, out var data))
            {
                data.IncrementAdminMessageCount();
                data.AdminMessagesSizeInBytes += message.ToString().Length;
            }
        }


        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.FromApp))]
        public void FromApp(Message message, SessionID sessionId)
        {
            if (_metricData.TryGetValue(sessionId, out var data))
            {
                data.IncrementAppMessageCount();
                data.AppMessagesSizeInBytes += message.ToString().Length;

                if (_random.NextDouble() < 0.01)
                {
                    Print(data);
                }
            }
        }

        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.OnLogon))]
        public void OnLogon(SessionID sessionId)
        {
            _metricData.Add(sessionId, new MetricData
            {
                StopWatch = Stopwatch.StartNew()
            });
        }

        [DiagnosticName(FixApp.ListenerName + "." + nameof(IApplication.OnLogout))]
        public void OnLogout(SessionID sessionId)
        {
            _metricData.Remove(sessionId);
        }

        private void Print(MetricData data)
        {
            Console.Clear();
            Console.WriteLine("------------------------- RECEIVING REPORT--------------------------------");
            Console.WriteLine($"Admin messages count:\t\t{data.AdminMessageCount}\nApplication messages count:\t\t{data.AppMessaageCount}\n");
            Console.WriteLine($"Admin messages size:\t\t{data.AdminMessagesSizeInBytes/ 1048576M} Mb\nApplication messages size:\t\t{data.AppMessagesSizeInBytes/ 1048576M} Mb\n");
            var sec = data.StopWatch.Elapsed.TotalSeconds;
            if (sec > 0)
            {
                Console.WriteLine($"Application messages throughput:\t\t{data.AppMessaageCount / sec} m/s\nApplication traffic throughput:\t\t{data.AppMessagesSizeInBytes / 1024 / sec} Kb/s\n");
                Console.WriteLine($"All messages throughput:\t\t{(data.AdminMessageCount + data.AppMessaageCount) / sec} m/s\nAll traffic throughput:\t\t{(data.AppMessagesSizeInBytes + data.AdminMessagesSizeInBytes ) / 1024 / sec} Kb/s\n");
            }
            var process = Process.GetCurrentProcess();
            Console.WriteLine($"Physical memory usage:\t\t{process.WorkingSet64 / 1048576M} Mb");
        }
    }
}
