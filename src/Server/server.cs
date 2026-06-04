using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.IO;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using Terminal.Gui;
using System.Linq;
using Grpc.Net.Client;
using Server.Grpc;
namespace Server
{
    class ServerApp
    {
        private static TcpListener _server = null!;
        private static bool _isRunning;
        private static List<string> _logBuffer = new List<string>();
        private static object _logLock = new object();

        private static Mutex _serverLogMutex = new Mutex();
        private static string _connectionString = "Data Source=servidor.db";

        // UI Elements
        private static ListView _logListView = null!;
        private static ListView _dbListView = null!;
        private static Label _statsLabel = null!;
        private static string _filterSensorId = "";
        private static string _filterZone = "";

        
        // Grpc
        private static GrpcChannel _grpcChannel = null!;
        private static StatisticsCalculator.StatisticsCalculatorClient _statisticsClient = null!;
        
        static void Main(string[] args)
        {
            InitializeDatabase();

            _server = new TcpListener(IPAddress.Any, 5000);
            _server.Start();
            _isRunning = true;

            _grpcChannel = GrpcChannel.ForAddress("http://localhost:50052");
            _statisticsClient = new StatisticsCalculator.StatisticsCalculatorClient(_grpcChannel);

            LogToConsole("[START] Servidor online (Porta 5000) - SQLite Iniciado");

            Thread listenerThread = new Thread(AcceptClients);
            listenerThread.IsBackground = true;
            listenerThread.Start();

            // Terminal.Gui Init
            Application.Init();
            var top = Application.Top;
            var window = new Window("Servidor Principal - SensorGrid") {
                X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill()
            };

            var tabView = new TabView() {
                X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill()
            };

            // Taba Database
            _dbListView = new ListView(new List<string>()) { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill() };
            tabView.AddTab(new TabView.Tab("Database", _dbListView), true);

            // Taba Logs
            _logListView = new ListView(_logBuffer) { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill() };
            tabView.AddTab(new TabView.Tab("Logs", _logListView), false);

            // Taba Estatísticas
            var statsView = new View() { X = 0, Y = 0, Width = Dim.Fill(), Height = Dim.Fill() };
            var lblSensor = new Label("Sensor ID:") { X = 1, Y = 1 };
            var txtSensor = new TextField("") { X = 15, Y = 1, Width = 20 };
            var lblZone = new Label("Zona:") { X = 1, Y = 3 };
            var txtZone = new TextField("") { X = 15, Y = 3, Width = 20 };
            var btnAnalisar = new Button("Analisar") { X = 1, Y = 5 };
            
            btnAnalisar.Clicked += () => {
                _filterSensorId = txtSensor.Text?.ToString() ?? "";
                _filterZone = txtZone.Text?.ToString() ?? "";
            };

            _statsLabel = new Label("A carregar estatísticas...") { X = 1, Y = 7, Width = Dim.Fill(), Height = Dim.Fill() - 7 };
            
            statsView.Add(lblSensor, txtSensor, lblZone, txtZone, btnAnalisar, _statsLabel);
            tabView.AddTab(new TabView.Tab("Estatísticas", statsView), false);

            window.Add(tabView);
            top.Add(window);

            Application.MainLoop.AddTimeout(TimeSpan.FromSeconds(2), (loop) => {
                UpdateUI();
                return true;
            });

            Application.Run();
            
            _isRunning = false;
            _server.Stop();
        }

        private static void UpdateUI()
        {
            // Logs
            lock (_logLock)
            {
                _logListView.SetSource(_logBuffer.ToList());
            }

            // Database
            var dbLines = new List<string>();
            dbLines.Add(string.Format("{0,-6} {1,-10} {2,-10} {3,-15} {4,-10} {5,-20}", "ID", "Gateway", "Sensor", "Tipo", "Valor", "Timestamp"));
            dbLines.Add(new string('-', 80));

            int totalRecords = 0;
            Dictionary<string, int> countByType = new Dictionary<string, int>();

            _serverLogMutex.WaitOne();
            try
            {
                using (var connection = new SqliteConnection(_connectionString))
                {
                    connection.Open();
                    
                    // Lemos ultimos 50 para o DB view
                    using (var command = new SqliteCommand("SELECT Id, GatewayId, SensorId, DataType, DataValue, Timestamp FROM SensorData ORDER BY Id DESC LIMIT 50", connection))
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            dbLines.Add(string.Format("{0,-6} {1,-10} {2,-10} {3,-15} {4,-10:F2} {5,-20}", 
                                reader.GetInt32(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetDouble(4), reader.GetString(5)));
                        }
                    }
                }
            }
            catch {}
            finally
            {
                _serverLogMutex.ReleaseMutex();
            }

            var statsText = new StringBuilder();
            try 
            {
                var req = new AnalysisRequest { SensorId = _filterSensorId, Zone = _filterZone };
                var reply = _statisticsClient.GetStatistics(req);
                totalRecords = reply.TotalRecords;
                
                if (!string.IsNullOrEmpty(_filterSensorId) || !string.IsNullOrEmpty(_filterZone)) {
                    statsText.AppendLine($"Filtro Ativo - Sensor: '{_filterSensorId}' | Zona: '{_filterZone}'");
                }
                statsText.AppendLine($"Total de Registos na Base de Dados: {totalRecords}\n");
                statsText.AppendLine("Estatísticas por Tipo:");
                foreach (var entry in reply.Entries)
                {
                    statsText.AppendLine($"- {entry.DataType}: {entry.Count} registos (Média: {entry.Average:F2})");
                }
                
                if (reply.Alerts.Count > 0) {
                    statsText.AppendLine("\n*** ALERTAS (Registados na DB) ***");
                    foreach(var alert in reply.Alerts) {
                        statsText.AppendLine($"! {alert}");
                    }
                }
            }
            catch (Exception ex)
            {
                statsText.AppendLine($"[ERRO gRPC Estatísticas] {ex.Message}");
            }

            _dbListView.SetSource(dbLines);
            statsText.AppendLine($"\nEstado Servidor TCP: {(_isRunning ? "Online" : "Offline")}");
            _statsLabel.Text = statsText.ToString();

            Application.Top.SetNeedsDisplay();
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
                        int bytesRead = stream.Read(buffer, 0, buffer.Length);
                        if (bytesRead == 0) break;

                        string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                        LogToConsole($"Recebido: {message.Replace("\n", " | ")}");
                        ProcessarEGuardarMensagem(message);
                    }
                    catch { break; }
                }
            }
        }

        private static void ProcessarEGuardarMensagem(string msg)
        {
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
                LogToConsole($"[ERRO DB] {ex.Message}");
            }
            finally
            {
                _serverLogMutex.ReleaseMutex();
            }
        }

        private static void LogToConsole(string message)
        {
            string logLine = $"[{DateTime.Now:HH:mm:ss}] {message}";
            lock (_logLock)
            {
                _logBuffer.Add(logLine);
                if (_logBuffer.Count > 100) _logBuffer.RemoveAt(0);
            }
        }
    }
}