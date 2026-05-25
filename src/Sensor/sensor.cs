using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Sensor
{
    public class DatasetRecord
    {
        public string Data { get; set; } = "";
        public string Hora { get; set; } = "";
        public double Precipitacao { get; set; }
        public double Pressao { get; set; }
        public double Temperatura { get; set; }
        public double Humidade { get; set; }
        public double Vento { get; set; }
    }

    class SensorApp
    {
        private static IConnection? _connection;
        private const string EXCHANGE_NAME = "urban_sensors";
        private static Dictionary<string, (string Zone, string[] Types)> _configuredSensors = new Dictionary<string, (string, string[])>();
        public static List<DatasetRecord> Dataset = new List<DatasetRecord>();

        static async Task Main(string[] args)
        {
            Console.Title = "GERENCIADOR DE SENSORES";
            
            await ConectarRabbitMQ();
            LoadConfig();
            LoadDataset();
            
            Console.WriteLine("Sensores disponíveis na configuração:");
            foreach (var kvp in _configuredSensors)
            {
                Console.WriteLine($" - {kvp.Key} | Zona: {kvp.Value.Zone} | Tipos: {string.Join(",", kvp.Value.Types)}");
            }

            Console.WriteLine("\nInsira os digitos do ID do sensor que deseja criar (Ex: 101 para S101), ou 'sair' para terminar:");

            while (true)
            {
                string? input = Console.ReadLine()?.Trim();
                if (string.IsNullOrEmpty(input)) continue;
                if (input.ToLower() == "sair") break;

                string fullId = "S" + input;
                if (_configuredSensors.TryGetValue(fullId, out var sensorInfo))
                {
                    SensorInstance sensor = new SensorInstance(fullId, sensorInfo.Zone, sensorInfo.Types, _connection, EXCHANGE_NAME);
                    Thread t = new Thread(() => sensor.Run().GetAwaiter().GetResult());
                    t.IsBackground = true;
                    t.Start();
                    Console.WriteLine($"[OK] Sensor {fullId} criado.");
                }
                else
                {
                    Console.WriteLine($"[ERRO] Sensor {fullId} não encontrado na configuração.");
                }
            }
        }

        private static void LoadDataset()
        {
            string datasetPath = Path.Combine("..", "..", "dataset_transformado.csv");
            if (!File.Exists(datasetPath))
            {
                Console.WriteLine($"[ERRO] Ficheiro {datasetPath} não encontrado!");
                return;
            }

            var lines = File.ReadAllLines(datasetPath);
            for (int i = 1; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i])) continue;
                var parts = lines[i].Split(',');
                if (parts.Length >= 7)
                {
                    if (double.TryParse(parts[2], System.Globalization.CultureInfo.InvariantCulture, out double precip) &&
                        double.TryParse(parts[3], System.Globalization.CultureInfo.InvariantCulture, out double pres) &&
                        double.TryParse(parts[4], System.Globalization.CultureInfo.InvariantCulture, out double temp) &&
                        double.TryParse(parts[5], System.Globalization.CultureInfo.InvariantCulture, out double hum) &&
                        double.TryParse(parts[6], System.Globalization.CultureInfo.InvariantCulture, out double vent))
                    {
                        Dataset.Add(new DatasetRecord
                        {
                            Data = parts[0],
                            Hora = parts[1],
                            Precipitacao = precip,
                            Pressao = pres,
                            Temperatura = temp,
                            Humidade = hum,
                            Vento = vent
                        });
                    }
                }
            }
            Console.WriteLine($"[INFO] Dataset carregado: {Dataset.Count} registos.");
        }

        private static void LoadConfig()
        {
            string configPath = Path.Combine("..", "Gateway", "sensores_config.csv");
            if (!File.Exists(configPath))
            {
                Console.WriteLine($"[ERRO] Ficheiro {configPath} não encontrado!");
                return;
            }

            foreach (var linha in File.ReadLines(configPath))
            {
                if (string.IsNullOrWhiteSpace(linha) || linha.StartsWith("#")) continue;

                var partes = linha.Split(':');
                if (partes.Length >= 4)
                {
                    string id = partes[0].Trim();
                    string estado = partes[1].Trim();
                    string zona = partes[2].Trim();
                    string tiposRaw = partes[3].Trim().Trim('[', ']');
                    string[] tipos = tiposRaw.Split(',').Select(t => t.Trim()).ToArray();
                    
                    if (estado.ToLower() == "ativo")
                    {
                        _configuredSensors[id] = (zona, tipos);
                    }
                    else if (estado.ToLower() == "desativado")
                    {
                        continue;
                    }
                }
            }
        }

        private static async Task ConectarRabbitMQ()
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            while (_connection == null)
            {
                try
                {
                    _connection = await factory.CreateConnectionAsync();
                    Console.WriteLine("[RABBITMQ] Conectado ao Broker.");
                }
                catch
                {
                    Console.WriteLine("[RABBITMQ] Offline. A tentar novamente em 3 segundos...");
                    await Task.Delay(3000);
                }
            }
        }
    }

    class SensorInstance
    {
        private string _sensorId;
        private string _zone;
        private string[] _supportedTypes;
        private IConnection? _connection;
        private IChannel? _channel;
        private string _exchangeName;
        private Random _rng;
        private int _currentLineIndex;

        public SensorInstance(string id, string zone, string[] types, IConnection? connection, string exchangeName)
        {
            _sensorId = id;
            _zone = zone;
            _supportedTypes = types;
            _connection = connection;
            _exchangeName = exchangeName;
            _rng = new Random();
            _currentLineIndex = _rng.Next(0, Math.Max(1, SensorApp.Dataset.Count));
        }

        public async Task Run()
        {
            await InitializeRabbitMQ();
            await RegistarNoGateway();

            _ = Task.Run(EnviarHeartbeatLoop);

            while (true)
            {
                if (SensorApp.Dataset.Count > 0)
                {
                    var record = SensorApp.Dataset[_currentLineIndex];

                    foreach (var type in _supportedTypes)
                    {
                        double value = 0;
                        switch (type)
                        {
                            case "PRECIPITACAO": value = record.Precipitacao; break;
                            case "PRESSAO": value = record.Pressao; break;
                            case "TEMPERATURA": value = record.Temperatura; break;
                            case "HUMIDADE": value = record.Humidade; break;
                            case "VENTO": value = record.Vento; break;
                        }

                        string dataMsg = $"OP:DATA | ID:{_sensorId} | ZONE:{_zone} | TYPE:{type} | VAL:{value:F1} | DATA:{record.Data} | HORA:{record.Hora}";
                        string routingKey = $"sensor.data.{_zone}.{type}";
                        await EnviarMensagem(routingKey, dataMsg);

                        Console.WriteLine($"[DADOS {_sensorId}] {type} = {value:F1} (em {record.Data} {record.Hora}) -> {routingKey}");
                    }

                    _currentLineIndex = (_currentLineIndex + 1) % SensorApp.Dataset.Count;
                }

                Thread.Sleep(10000);
            }
        }

        private async Task InitializeRabbitMQ()
        {
            if (_connection != null)
            {
                _channel = await _connection.CreateChannelAsync();
                await _channel.ExchangeDeclareAsync(exchange: _exchangeName, type: ExchangeType.Topic);
            }
        }

        private async Task RegistarNoGateway()
        {
            try
            {
                string typesList = string.Join(",", _supportedTypes);
                string regMsg = $"OP:REG | ID:{_sensorId} | TYPES:[{typesList}] | ZONE:{_zone}";
                
                string routingKey = "sensor.registration";
                await EnviarMensagem(routingKey, regMsg);
                Console.WriteLine($"[REG {_sensorId}] Registo enviado.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO {_sensorId}] Falha ao registar: {ex.Message}");
            }
        }

        private async Task EnviarHeartbeatLoop()
        {
            while (true)
            {
                string hbMsg = $"OP:HB | ID:{_sensorId} | TS:{DateTime.Now:s}";
                await EnviarMensagem("sensor.heartbeat", hbMsg);
                Thread.Sleep(10000);
            }
        }

        private async Task EnviarMensagem(string routingKey, string msg)
        {
            try
            {
                if (_channel != null)
                {
                    byte[] body = Encoding.UTF8.GetBytes(msg);
                    await _channel.BasicPublishAsync(
                        exchange: _exchangeName,
                        routingKey: routingKey,
                        body: body
                    );
                }
            }
            catch
            {
                Console.WriteLine($"[ERRO {_sensorId}] Conexão perdida ao publicar!");
            }
        }
    }
}