using System.Runtime.InteropServices;
using Xunit;

namespace FastTests;

public class NonMacFactAttribute : FactAttribute
{
    public NonMacFactAttribute()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            Skip = "Test cannot be run on Mac machine";
        }
    }
}
