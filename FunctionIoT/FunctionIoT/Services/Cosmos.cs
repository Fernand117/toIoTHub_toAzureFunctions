using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FunctionIoT.Services
{
    internal class Cosmos
    {
        private static readonly string cosmosEndpoint = "https://dv-iot-cosmosdb.documents.azure.com:443/";
        private static readonly string cosmosKey = "ooqVEhBmAtSdYhpd2vJaZq5sXbr0uzNbO1hLJXB7ngvS3QDKcLHKdeQd6lujWp7wAVEC04zER4XnACDbVmhUWw==";
        private static readonly string cosmosDatabase = "ToDoList";
        private static readonly string cosmosContainer = "Items";

        private static ThreadStart CosmosThreadStart;
        private static Thread CosmosThread;

        public static void RunProccess()
        {
            try
            {
                if (CosmosThread == null)
                {
                    CosmosThreadStart = new ThreadStart(CosmosProccess);
                    CosmosThread = new Thread(CosmosThreadStart);
                    CosmosThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR-COSMOS-THREAD: " + ex.Message);
            }
        }

        public static async void CosmosProccess()
        {
            try
            {
                using CosmosClient client = new
                (
                    accountEndpoint: cosmosEndpoint,
                    authKeyOrResourceToken: cosmosKey,
                    clientOptions: new CosmosClientOptions() { AllowBulkExecution = true }
                );

                Container container = client.GetDatabase(id: cosmosDatabase).GetContainer(id: cosmosContainer);

                while (true)
                {
                    if (!IoTHub.isEventHubOffsetInit)
                    {
                        var query = new QueryDefinition
                        (
                            query: "SELECT TOP 1 p.fecha_insersion FROM Items p ORDER BY p.fecha_insersion DESC"
                        );

                        using FeedIterator<dynamic> feed = container.GetItemQueryIterator<dynamic>
                        (
                            queryDefinition: query
                        );

                        while (feed.HasMoreResults)
                        {
                            var cancellationSource = new CancellationTokenSource();
                            cancellationSource.CancelAfter(5000);

                            try
                            {
                                FeedResponse<dynamic> response = await feed.ReadNextAsync(cancellationSource.Token);

                                foreach (JObject item in response)
                                {
                                    DateTime date = item.Value<DateTime>("fecha_insersion");
                                    IoTHub.EventHubOffset = date.ToUniversalTime();
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("ERROR-FEEDMORERESULTS: " + ex.Message);
                            }
                        }

                        IoTHub.isEventHubOffsetInit = true;

                        if (Stream.streamList.Count > 0)
                        {
                            DateTime init = DateTime.Now;
                            List<Task> taskList = new();
                            List<JObject> documentList = new();
                            int count = 0;

                            var cancellationSource = new CancellationTokenSource();
                            cancellationSource.CancelAfter(5000);

                            foreach (JObject doc in documentList)
                            {
                                try
                                {
                                    taskList.Add(container.CreateItemAsync
                                        (
                                            item: doc,
                                            cancellationToken: cancellationSource.Token
                                        ).ContinueWith(itemResponse =>
                                        {
                                            if (itemResponse.IsFaulted)
                                            {
                                                AggregateException innerExceptions = itemResponse.Exception.Flatten();
                                                if ((innerExceptions.InnerExceptions.FirstOrDefault(innexEx => innexEx is CosmosException) is CosmosException cosmosException))
                                                {
                                                    Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message})");
                                                }
                                                else
                                                {
                                                    Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                                                }
                                            }
                                            else if (!itemResponse.IsCanceled)
                                            {
                                                if (itemResponse.Result != null)
                                                {
                                                    string id = itemResponse.Result.Resource.Value<string>("id");
                                                    JObject documentCreated = documentList.Find(d => d.Value<string>("id").Equals(id));
                                                    documentList.Remove(documentCreated);
                                                    count++;
                                                }
                                            }
                                        })
                                    );
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("ERROR-STREAM: " + ex.Message);
                                }
                            }

                            await Task.WhenAll(taskList);

                            if (documentList.Count == 0)
                            {
                                Stream.streamList.Remove(documentList);
                            }

                            Console.WriteLine("Insert " + count + " documents in: " + (DateTime.Now - init).TotalMilliseconds + " miliseconds");
                        }
                        Thread.Sleep(100);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR-COSMOSPROCCESS: " + ex.Message);
            }
            finally
            {
                CosmosThread = null;
                Thread.Sleep(1000);
                RunProccess();
            }
        }
    }
}
