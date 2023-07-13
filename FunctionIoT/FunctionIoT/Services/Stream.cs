using FunctionIoT.Models;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace FunctionIoT.Services
{
    internal class Stream
    {
        private static ThreadStart StreamThreadStart;
        private static Thread StreamThread;

        public static List<List<JObject>> streamList = new();

        public static void RunProccess()
        {
            try
            {
                if (StreamThread == null)
                {
                    StreamThreadStart = new ThreadStart(StreamProccess);
                    StreamThread = new Thread(StreamThreadStart);
                    StreamThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR-RUNPROCCESS: " + ex.Message);
            }
        }

        public static void StreamProccess()
        {
            try
            {
                while (true)
                {
                    if (IoTHub.IoTHubDataList.Count > 0 && Variable.isListAvailable)
                    {
                        List<Variable> variables = Variable.variableList.ToList();

                        if (IoTHub.IoTHubDataList.Count < 10) Thread.Sleep(2000);

                        DateTime init = DateTime.Now;

                        var data = IoTHub.IoTHubDataList.First();
                        List<JObject> docs = new();

                        foreach (Variable variable in variables)
                        {
                            Value value = data.values.Find(x => x.id.Equals(variable.tagName));

                            if (value != null)
                            {
                                Value newValue = new Value()
                                {
                                    id = value.id,
                                    v = value.v,
                                    variable = variable
                                };

                                data.values.Add(newValue);
                            }
                            else
                            {
                                value.variable = variable;
                            }
                        }

                        var groupedValues = data.values.GroupBy(x => x.variable.idLine);

                        foreach (var group in groupedValues)
                        {
                            if (group.Key > 0)
                            {
                                JObject doc = new()
                                {
                                    new JProperty("id", group.Key.ToString() + "-" + data.dateTime.ToUniversalTime().Ticks.ToString()),
                                    new JProperty("fecha_insercion", data.dateTime.ToUniversalTime()),
                                    new JProperty("idLinea", group.Key)
                                };

                                foreach (Value value in group)
                                {
                                    try
                                    {
                                        switch (value.variable.type)
                                        {
                                            case 1:
                                                double doubleValue = 0;

                                                try
                                                {
                                                    doubleValue = (double)Convert.ToDecimal(value.v);
                                                    doc.Add(new JProperty(value.variable.fieldName, doubleValue));
                                                }
                                                catch (Exception ex)
                                                {
                                                    Console.WriteLine("ERROR-CASE1-VALUE: " + ex.Message);
                                                }

                                                break;

                                            case 2:
                                                bool booleanValue = false;

                                                try
                                                {
                                                    booleanValue = Convert.ToBoolean(value.v);
                                                    doc.Add(new JProperty(value.variable.fieldName, booleanValue));
                                                }
                                                catch (Exception ex)
                                                {
                                                    Console.WriteLine("ERROR-CASE2-VALUE: " + ex.Message);
                                                }

                                                break;

                                            case 3:
                                                string stringValue = value.v.Trim();
                                                doc.Add(new JProperty(value.variable.fieldName, stringValue));
                                                break;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine("ERROR-VALUE: " + ex.Message);
                                    }
                                }

                                docs.Add(doc);
                            }
                        }

                        streamList.Add(docs);
                        IoTHub.IoTHubDataList.RemoveAt(0);
                    }
                    Thread.Sleep(100);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR-STREAMPROCCESS: " + ex.Message);
            }
            finally
            {
                StreamThread = null;
                Thread.Sleep(1000);
                RunProccess();
            }
        }
    }
}
