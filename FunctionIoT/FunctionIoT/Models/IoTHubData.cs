using System;
using System.Collections.Generic;

namespace FunctionIoT.Models
{
    internal class IoTHubData
    {
        public DateTime dateTime;
        public List<Value> values = new List<Value>();
    }

    public class Value
    {
        public string id;
        public string v;
        public bool q;

        public Variable variable = new Variable();
    }
}
