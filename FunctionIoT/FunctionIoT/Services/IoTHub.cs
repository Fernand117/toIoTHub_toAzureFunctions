using Azure.Messaging.EventHubs.Consumer;
using FunctionIoT.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace FunctionIoT.Services
{
    internal class IoTHub
    {
        private static readonly string consumerGroup = "dv-iot-test-group";
        private static readonly string devideId = "dv-iot-test";
        private static readonly string connectionString = $"Endpoint=sb://ihsuprodblres061dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=/MtSqKBVqnPSdoWKW7sKOItCPcd4a7j/x4PwD2gnKBk=;EntityPath=iothub-ehub-dev-iot-te-25136175-05eac7bc52";

        private static ThreadStart IoTHubThreadStart;
        private static Thread IoThubThread;

        public static DateTimeOffset EventHubOffset = DateTimeOffset.Now;
        public static bool isEventHubOffsetInit = false;
        public static List<IoTHubData> IoTHubDataList = new();

        public static void RunProccess()
        {
            try
            {
                if (IoThubThread == null)
                {
                    IoTHubThreadStart = new ThreadStart(IoTHubProccess);
                    IoThubThread = new Thread(IoTHubThreadStart);
                    IoThubThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR-THREAD-START: " + ex.Message);
            }
        }

        private static async void IoTHubProccess()
        {
            try
            {
                while (true)
                {
                    if (isEventHubOffsetInit)
                    {
                        await using EventHubConsumerClient consumer = new EventHubConsumerClient(consumerGroup, connectionString);

                        EventPosition startingPosition = EventPosition.FromEnqueuedTime(EventHubOffset);
                        string partitionId = (await consumer.GetPartitionIdsAsync())[0];

                        await foreach (PartitionEvent partitionEvent in consumer.ReadEventsFromPartitionAsync(partitionId, startingPosition))
                        {
                            partitionEvent.Data.SystemProperties.TryGetValue("iothub-connection-device-id", out object currectDeviceID);

                            if (currectDeviceID.Equals(devideId))
                            {
                                DateTime init = DateTime.Now;
                                DateTime dateTime = partitionEvent.Data.EnqueuedTime.LocalDateTime;

                                IoTHubData IoTData = new IoTHubData();
                                IoTHubData lastIoTData = IoTHubDataList.LastOrDefault();
                                string json = Encoding.UTF8.GetString(partitionEvent.Data.EventBody.ToArray());

                                if (dateTime > (lastIoTData != null ? lastIoTData.dateTime.AddSeconds(3) : DateTime.MinValue))
                                {
                                    IoTData = JsonConvert.DeserializeObject<IoTHubData>(json);
                                    IoTData.dateTime = dateTime;
                                    IoTHubDataList.Add(IoTData);
                                }
                                else
                                {
                                    IoTData = lastIoTData;
                                    IoTHubData newIoTHubData = JsonConvert.DeserializeObject<IoTHubData>(json);

                                    foreach (Value value in newIoTHubData.values)
                                    {
                                        IoTData.values.Add(value);
                                    }
                                }

                                EventHubOffset = partitionEvent.Data.EnqueuedTime;
                            }
                        }
                        await consumer.DisposeAsync();
                    }
                    Thread.Sleep(1000);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR-IOTHUB-PROCCESS: " + ex.Message);
            }
            finally
            {
                IoThubThread = null;
                Thread.Sleep(1000);
                RunProccess();
            }
        }
    }
}
