using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            //XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 10_000; i++)
            {
                 Console.WriteLine($"Starting to run {i}");
                try
                {
                    //using (var testOutputHelper = new ConsoleTestOutputHelper())
                    //using (var test = new FirstClassPatch(testOutputHelper))
                    //{
                    //     test.PatchNullField_ExpectFieldSetToNull();
                    //}
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        }
    }
}
