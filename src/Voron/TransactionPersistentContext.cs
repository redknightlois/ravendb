using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Voron.Impl;

namespace Voron
{
    public sealed class TransactionPersistentContext(bool longLivedTransactions = true)
    {
        public bool LongLivedTransactions { get; set; } = longLivedTransactions;


        private readonly Stack<PageLocator> _pageLocators = new();


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal PageLocator AllocatePageLocator()
        {
            PageLocator locator;
            if (_pageLocators.Count != 0)
            {
                locator = _pageLocators.Pop();
                locator.Renew();
            }
            else
            {
                locator = new PageLocator();
            }
            return locator;
        }

        internal void FreePageLocator(PageLocator locator)
        {
            Debug.Assert(locator != null);
            if (_pageLocators.Count < 1024)
                _pageLocators.Push(locator);
        }
    }
}
