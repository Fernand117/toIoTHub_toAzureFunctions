using FunctionIoT.Models;
using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace FunctionIoT.Services
{
    internal class SQL
    {
        public static SqlConnection connection;
        public static string connectionString = System.Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING", EnvironmentVariableTarget.Process);

        private static ThreadStart SQLThreadStart;
        private static Thread SQLThread;

        private static int hoursToVariableListUpdate = 1;
        private static DateTime lastVariableListUpdate = DateTime.Now.AddHours(-hoursToVariableListUpdate);

        public static void RunProccess()
        {
            try
            {
                if (connection != null)
                {
                    connection.Dispose();
                }

                if (SQLThread == null)
                {
                    SQLThreadStart = new ThreadStart(SQLProccess);
                    SQLThread = new Thread(SQLThreadStart);
                    SQLThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR-RUN-SQLPROCCESS: " + ex.Message);
            }
        }

        private static async void SQLProccess()
        {
            try
            {
                while (true)
                {
                    if (lastVariableListUpdate.AddHours(hoursToVariableListUpdate) < DateTime.Now)
                    {
                        lastVariableListUpdate = DateTime.Now;

                        string query = "SELECT  idLinea, c_field_name, c_tag, idTipoDato FROM datos_cs_iot";

                        SqlDataReader reader = await Read(query);

                        if (reader != null)
                        {
                            Variable.isListAvailable = false;
                            Variable.variableList.Clear();

                            while (reader.Read())
                            {
                                Variable variable = new Variable()
                                {
                                    idLine = reader.GetInt32(0),
                                    fieldName = reader.GetString(1),
                                    tagName = reader.GetString(2),
                                    type = reader.GetInt32(3)
                                };

                                Variable.variableList.Add(variable);
                            }

                            Variable.isListAvailable = true;
                            reader.Close();
                        }
                    }
                    Thread.Sleep(5000);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR-SQLPROCCESS: " + ex.Message);
            }
            finally
            {
                SQLThread = null;
                Thread.Sleep(1000);
                RunProccess();
            }
        }

        private static async Task<SqlDataReader> Read(string query)
        {
            SqlDataReader response = null;
            try
            {
                connection ??= new SqlConnection(connectionString);

                if (connection.State != System.Data.ConnectionState.Open)
                {
                    CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                    cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(5000));

                    await connection.OpenAsync(cancellationTokenSource.Token);
                }

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                    cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(5000));

                    response = await command.ExecuteReaderAsync(cancellationTokenSource.Token);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR-READ-SQL: " + ex.Message);

                if (connection != null)
                {
                    connection.Close();
                    connection.Dispose();
                    connection = null;
                }
            }

            return response;
        }
    }
}
