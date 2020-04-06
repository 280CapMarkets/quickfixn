using System;
using System.Threading;
using System.Threading.Tasks;

namespace QuickFix.Util
{
    public sealed class AwaitableCriticalSection : IDisposable
    {
        private readonly SemaphoreSlim m_semaphore = new SemaphoreSlim(1, 1);
        private readonly Task<IDisposable> m_releaser;

        public AwaitableCriticalSection()
        {
            m_releaser = Task.FromResult((IDisposable)new Releaser(this));
        }

        public Task<IDisposable> EnterAsync()
        {
            var wait = m_semaphore.WaitAsync();
            return wait.IsCompleted ?
                m_releaser :
                wait.ContinueWith((_, state) => (IDisposable)state,
                    m_releaser.Result, CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }

        public void Dispose()
        {
            m_semaphore.Dispose();
        }

        #region private nested classes

        private sealed class Releaser : IDisposable
        {
            private readonly AwaitableCriticalSection m_toRelease;
            internal Releaser(AwaitableCriticalSection toRelease) { m_toRelease = toRelease; }
            public void Dispose() { m_toRelease.m_semaphore.Release(); }
        }

        #endregion
    }
}
