using System.Diagnostics;
using System.Threading;

namespace FakeFix.Common.Diagnostic.Models
{
    internal class MetricData
    {
        private int _adminMessageCount;
        private int _appMessageCount;
        public Stopwatch StopWatch { get; set; }
        public int AdminMessageCount => _adminMessageCount;
        public int AppMessaageCount => _appMessageCount;
        public long AdminMessagesSizeInBytes { get; set; }
        public long AppMessagesSizeInBytes { get; set; }

        public int IncrementAdminMessageCount() => Interlocked.Increment(ref _adminMessageCount);

        public int IncrementAppMessageCount() => Interlocked.Increment(ref _appMessageCount);
    }
}
