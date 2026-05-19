using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.IO;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace Server
{
    class ServerApp
    {
        private static TcpListener _server;
        private static bool _isRunning;

        // Mutex para evitar acessos concorrentes simultâneos pesados à BD
        private static Mutex _serverLogMutex = new Mutex();
        private static string _connectionString = "Data Source=servidor.db";

        static void Main(string[] args)
        {
            Console.Title = "SERVIDOR PRINCIPAL";

            InitializeDatabase();

            _server = new TcpListener(IPAddress.Any, 5000);
            _server.Start();
            _isRunning = true;

            Console.WriteLine("[START] Servidor online (Porta 5000) - SQLite Iniciado");

            // Atendimento Concorrente com Thread principal
            Thread listenerThread = new Thread(AcceptClients);
            listenerThread.IsBackground = true;
            listenerThread.Start();

            Console.WriteLine("Pressione Enter para encerrar o servidor...");
            Console.ReadLine();

            _isRunning = false;
            _server.Stop();
            Console.WriteLine("[STOP] Servidor offline");
        }

        private static void InitializeDatabase()
        {
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();
                string createTableQuery = @"
                    CREATE TABLE IF NOT EXISTS SensorData (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        GatewayId TEXT,
                        SensorId TEXT,
                        Zone TEXT,
                        DataType TEXT,
                        DataValue REAL,
                        Timestamp TEXT
                    )";
                using (var command = new SqliteCommand(createTableQuery, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void AcceptClients()
        {
            while (_isRunning)
            {
                try
                {
                    TcpClient client = _server.AcceptTcpClient();

                    // Nova Thread para cada Gateway que se ligue
                    Thread clientThread = new Thread(() => HandleClientData(client));
                    clientThread.IsBackground = true;
                    clientThread.Start();
                }
                catch
                {
                    if (!_isRunning) break;
                }
            }
        }

        private static void HandleClientData(TcpClient client)
        {
            using (client)
            using (NetworkStream stream = client.GetStream())
            {
                byte[] buffer = new byte[4096];
                while (_isRunning)
                {
                    try
                    {
                        // Leitura síncrona
                        int bytesRead = stream.Read(buffer, 0, buffer.Length);
                        if (bytesRead == 0) break;

                        string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                        
                        // Log na consola
                        LogToConsole($"Recebido:\n{message}");
                        
                        ProcessarEGuardarMensagem(message);
                    }
                    catch { break; }
                }
            }
        }

        private static void ProcessarEGuardarMensagem(string msg)
        {
            // Divide a mensagem em linhas (ignora linhas vazias)
            var linhas = msg.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var linha in linhas)
            {
                var campos = linha.Split('|', StringSplitOptions.RemoveEmptyEntries);
                string gtwId = "", sensorId = "", zone = "", type = "", timestamp = "";
                double val = 0.0;

                foreach (var campo in campos)
                {
                    var c = campo.Trim();
                    if (c.StartsWith("GTW_ID:")) gtwId = c.Substring(7).Trim();
                    else if (c.StartsWith("ID:")) sensorId = c.Substring(3).Trim();
                    else if (c.StartsWith("ZONE:")) zone = c.Substring(5).Trim();
                    else if (c.StartsWith("TYPE:")) type = c.Substring(5).Trim();
                    else if (c.StartsWith("VAL:"))
                    {
                        string valStr = c.Substring(4).Trim().Replace(",", ".");
                        double.TryParse(valStr, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out val);
                    }
                    else if (c.StartsWith("TS:")) timestamp = c.Substring(3).Trim();
                }

                if (!string.IsNullOrEmpty(sensorId) && !string.IsNullOrEmpty(type))
                {
                    GuardarNaBaseDeDados(gtwId, sensorId, zone, type, val, timestamp);
                }
            }
        }

        private static void GuardarNaBaseDeDados(string gtwId, string sensorId, string zone, string type, double val, string timestamp)
        {
            _serverLogMutex.WaitOne();
            try
            {
                using (var connection = new SqliteConnection(_connectionString))
                {
                    connection.Open();
                    string insertQuery = @"
                        INSERT INTO SensorData (GatewayId, SensorId, Zone, DataType, DataValue, Timestamp)
                        VALUES (@GatewayId, @SensorId, @Zone, @DataType, @DataValue, @Timestamp)";
                    
                    using (var command = new SqliteCommand(insertQuery, connection))
                    {
                        command.Parameters.AddWithValue("@GatewayId", gtwId);
                        command.Parameters.AddWithValue("@SensorId", sensorId);
                        command.Parameters.AddWithValue("@Zone", zone);
                        command.Parameters.AddWithValue("@DataType", type);
                        command.Parameters.AddWithValue("@DataValue", val);
                        command.Parameters.AddWithValue("@Timestamp", timestamp);
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO DB] Falha ao guardar registo: {ex.Message}");
            }
            finally
            {
                _serverLogMutex.ReleaseMutex();
            }
        }

        private static void LogToConsole(string message)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {message}");
        }
    }
}