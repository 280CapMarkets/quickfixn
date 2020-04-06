using System;
using System.Threading;
using System.Threading.Tasks;
using FakeFix.Common;
using FakeFix.Common.Diagnostic;
using QuickFix;
using QuickFix.Fields;
using MDIR = QuickFix.FIX50SP2.MarketDataIncrementalRefresh;

namespace FakeFix.Client
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var componentFactory = new BaseComponentsFactory();
            using var cancellationTokenSource = new CancellationTokenSource();
            using var subscription = componentFactory.CreateDiagnosticSubscription( new ReceiverDiagnosticObserver());
            using var app = componentFactory.CreateApp(cancellationTokenSource/*, CreateFakePartner1Message*/);
            using var client = componentFactory.CreateFixClient(app, @".\Config\partner1\client.cfg");
            var rootTask = client.Start(cancellationTokenSource.Token);
            Console.WriteLine("Client started! Press Enter to Stop");
            Console.ReadLine();
            cancellationTokenSource.Cancel(false);
            await app.WhenStopped();
            await rootTask;
        }

        /// <summary>
        /// Create MarketDataIncrementalRefresh message for fake UBS server
        /// </summary>
        /// <param name="sessionId">In case when server has more than one session, this param can be use to differentiate sessions</param>
        /// <returns></returns>
        private static Message CreateFakePartner1Message(string sessionId)
        {
            var msg = new MDIR();

            var group = new MDIR.NoMDEntriesGroup
            {
                MDUpdateAction = new MDUpdateAction(MDUpdateAction.CHANGE),
                MarketDepth = new MarketDepth(1),
                MDEntryType = new MDEntryType(MDEntryType.BID),
                MinQty = new MinQty(1000),
                MDEntryID = new MDEntryID("US36962G4B75_BID_1"),
                SecurityID = new SecurityID("US36962G4B75"),
                SecurityIDSource = new SecurityIDSource(SecurityIDSource.ISIN_NUMBER),
                PriceType = new PriceType(PriceType.PERCENTAGE),
                MDEntryPx = new MDEntryPx(100.4M)
            };
            msg.SetField(new MDReqID(Guid.NewGuid().ToString("N")));

            group.SetField(new DecimalField(5004, 132.531M));
            group.SetField(new DecimalField(5005, .1M));
            group.MDEntrySize = new MDEntrySize(260000);
            group.MDEntryDate = new MDEntryDate(new DateTime(2019, 11, 8, 0, 0, 0, DateTimeKind.Utc));
            group.MDEntryTime = new MDEntryTime(new DateTime(2019, 11, 8, 15, 37, 16, DateTimeKind.Utc).AddMilliseconds(442), true);
            group.SetField(new StringField(6373, "F"));
            group.Spread = new Spread(100.16M);
            msg.AddGroup(group);
            return msg;
        }
    }
}
