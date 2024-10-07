using System.Runtime.CompilerServices;

namespace Sparrow
{
    internal static class Branchless
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe int ToInt32(this bool value)
        {
            return *(byte*)&value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T ConditionalSelect<T>(bool condition, T ifWhen, T elseWhen) where T : unmanaged => condition ? ifWhen : elseWhen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe T* ConditionalSelect<T>(bool condition, T* ifWhen, T* elseWhen) where T : unmanaged => condition ? ifWhen : elseWhen;
    }
}
