using FunctionIoT.Services;
using System;

namespace FunctionIoT
{
    internal class Function
    {
        static void Main(string[] args)
        {
            IoTHub.RunProccess();
            Stream.RunProccess();
            SQL.RunProccess();
            Cosmos.RunProccess();
            Console.ReadLine();
        }
    }
}
