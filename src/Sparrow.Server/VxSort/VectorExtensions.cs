using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace VxSort
{
    internal static class VectorExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static Vector256<double> i2d<W>(Vector256<W> v) where W : unmanaged
        {
            return Vector256.AsDouble(v);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static Vector256<float> i2s<W>(Vector256<W> v) where W : unmanaged
        {
            return Vector256.AsSingle(v);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static Vector256<W> d2i<W>(Vector256<double> v) where W : unmanaged
        {
            if (typeof(W) == typeof(int))
            {
                return (Vector256<W>)(object)Vector256.AsInt32(v);
            }
            else if (typeof(W) == typeof(long))
            {
                return (Vector256<W>)(object)Vector256.AsInt64(v);
            }
            else if (typeof(W) == typeof(uint))
            {
                return (Vector256<W>)(object)Vector256.AsUInt32(v);
            }
            else if (typeof(W) == typeof(ulong))
            {
                return (Vector256<W>)(object)Vector256.AsUInt64(v);
            }

            throw new NotSupportedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static Vector256<W> s2i<W>(Vector256<float> v) where W : unmanaged
        {
            if (typeof(W) == typeof(int))
            {
                return (Vector256<W>)(object)Vector256.AsInt32(v);
            }
            else if (typeof(W) == typeof(long))
            {
                return (Vector256<W>)(object)Vector256.AsInt64(v);
            }
            else if (typeof(W) == typeof(uint))
            {
                return (Vector256<W>)(object)Vector256.AsUInt32(v);
            }
            else if (typeof(W) == typeof(ulong))
            {
                return (Vector256<W>)(object)Vector256.AsUInt64(v);
            }

            throw new NotSupportedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static Vector256<double> s2d(Vector256<float> v)
        {
            return Vector256.AsDouble(v);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static Vector256<float> d2s(Vector256<double> v)
        {
            return Vector256.AsSingle(v);
        }
    }
}
