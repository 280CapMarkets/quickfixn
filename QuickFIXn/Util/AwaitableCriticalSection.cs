using System;
using System.Threading;
using System.Threading.Tasks;

namespace QuickFix.Util
{
    public sealed class AwaitableCriticalSection : IDisposable
    {
        private static readonly AsyncLocal<bool> IsNested = new AsyncLocal<bool>();
        private static readonly Task<IDisposable> SubUnlocker = Task.FromResult<IDisposable>(new NestedUnlocker());
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        private readonly Task<IDisposable> _rootUnlocker;
        private readonly bool _isNestedSupported;

        public AwaitableCriticalSection(bool isNestedSupported)
        {
            _isNestedSupported = isNestedSupported;
            _rootUnlocker = Task.FromResult<IDisposable>(new Unlocker(this));
        }

        public Task<IDisposable> EnterAsync(CancellationToken cancellationToken)
        {
            if (_isNestedSupported && IsNested.Value) return SubUnlocker;
            var wait = _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (wait.IsCompleted) return _rootUnlocker;
                if (!IsNested.Value)
                    return wait.ContinueWith((_, state) => (IDisposable)state,
                        _rootUnlocker.Result, cancellationToken,
                        TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                throw new InvalidOperationException("Nested lock doesn't support");
            }
            finally
            {
                if (!IsNested.Value) IsNested.Value = true;
            }
        }

        public void Dispose()
        {
            _semaphore.Dispose();
        }

        #region private nested classes

        private sealed class Unlocker : IDisposable
        {
            private readonly AwaitableCriticalSection _toRelease;
            internal Unlocker(AwaitableCriticalSection toRelease) { _toRelease = toRelease; }
            public void Dispose()
            {
                IsNested.Value = false;
                _toRelease._semaphore.Release();
            }
        }

        private sealed class NestedUnlocker : IDisposable
        {
            public void Dispose() { }
        }

        #endregion
    }
}
