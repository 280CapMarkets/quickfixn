using System;
using System.Threading.Tasks;
using QuickFix;

namespace FakeFix.Common.Interfaces
{
    public interface IFixApp : IApplication, IDisposable
    {
        Task WhenStopped();
    }
}