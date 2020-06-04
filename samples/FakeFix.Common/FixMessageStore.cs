using System;
using System.Collections.Generic;
using QuickFix;

namespace FakeFix.Common
{
    internal class FixMessageStore : IMessageStore
    {
        private int _nextSenderMsgSeqNum;
        private int _nextTargetMsgSeqNum;

        public DateTime? CreationTime { get; private set; }

        public FixMessageStore()
        {
            Reset();
        }
        public void Dispose() { }

        public void Get(int startSeqNum, int endSeqNum, List<string> messages){}

        public bool Set(int msgSeqNum, string msg) => true;

        public int GetNextSenderMsgSeqNum() => _nextSenderMsgSeqNum;

        public int GetNextTargetMsgSeqNum() => _nextTargetMsgSeqNum;

        public void SetNextSenderMsgSeqNum(int value) => _nextSenderMsgSeqNum = value;

        public void SetNextTargetMsgSeqNum(int value) => _nextTargetMsgSeqNum = value;

        public void IncrNextSenderMsgSeqNum() => ++_nextSenderMsgSeqNum;

        public void IncrNextTargetMsgSeqNum() => ++_nextTargetMsgSeqNum;

        public DateTime GetCreationTime() => CreationTime ?? DateTime.UtcNow;

        public void Reset()
        {
            CreationTime = DateTime.UtcNow;
            _nextSenderMsgSeqNum = _nextTargetMsgSeqNum = 1;
        } 

        public void Refresh() { }

        
    }
}
