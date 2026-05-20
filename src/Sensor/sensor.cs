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
    class SensorApp
    {
        private static IConnection _connection;
        private const string EXCHANGE_NAME = "urban_sensors";
        private static Dictionary<string, (string Zone, string[] Types)> _configuredSensors = new Dictionary<string, (string, string[])>();

        static async Task Main(string[] args)
        {
            Console.Title = "GERENCIADOR DE SENSORES";
            
            await ConectarRabbitMQ();
            LoadConfig();
            
            Console.WriteLine("Sensores disponíveis na configuração:");
            foreach (var kvp in _configuredSensors)
            {
                Console.WriteLine($" - {kvp.Key} | Zona: {kvp.Value.Zone} | Tipos: {string.Join(",", kvp.Value.Types)}");
            }

            Console.WriteLine("\nInsira os digitos do ID do sensor que deseja criar (Ex: 101 para S101), ou 'sair' para terminar:");

            while (true)
            {
                string input = Console.ReadLine()?.Trim();
                if (string.IsNullOrEmpty(input)) continue;
                if (input.ToLower() == "sair") break;

                string fullId = "S" + input;
                if (_configuredSensors.TryGetValue(fullId, out var sensorInfo))
                {
                    SensorInstance sensor = new SensorInstance(fullId, sensorInfo.Zone, sensorInfo.Types, _connection, EXCHANGE_NAME);
                    Thread t = new Thread((sensor.Run()).GetAwaiter().GetResult);
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
        private IConnection _connection;
        private IChannel _channel;
        private string _exchangeName;
        private Random _rng;

        public SensorInstance(string id, string zone, string[] types, IConnection connection, string exchangeName)
        {
            _sensorId = id;
            _zone = zone;
            _supportedTypes = types;
            _connection = connection;
            _exchangeName = exchangeName;
            _rng = new Random();
        }

        public async Task Run()
        {
            InitializeRabbitMQ().GetAwaiter().GetResult();
            RegistarNoGateway().GetAwaiter().GetResult();

            Thread hbThread = new Thread(EnviarHeartbeatLoop().GetAwaiter().GetResult);
            hbThread.IsBackground = true;
            hbThread.Start();

            while (true)
            {
                var measurements = GerarDadosSensor();
                foreach (var measurement in measurements)
                {
                    string type = measurement.Key;
                    double value = measurement.Value;
                    string dataMsg = $"OP:DATA | ID:{_sensorId} | ZONE:{_zone} | TYPE:{type} | VAL:{value:F1} | TS:{DateTime.Now:s}";

                    string routingKey = $"sensor.data.{_zone}.{type}";
                    await EnviarMensagem(routingKey, dataMsg);
                    
                    Console.WriteLine($"[DADOS {_sensorId}] {type} = {value:F1} -> {routingKey}");
                }

                Thread.Sleep(5000);
            }
        }

        private async Task InitializeRabbitMQ()
        {
            _channel = await _connection.CreateChannelAsync();
            await _channel.ExchangeDeclareAsync(exchange: _exchangeName, type: ExchangeType.Topic);
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

        private IDictionary<string, double> GerarDadosSensor()
        {
            var data = new Dictionary<string, double>();
            foreach(var type in _supportedTypes)
            {
                double value = 0.0;
                switch(type)
                {
                    case "TEMP": value = Math.Round(_rng.NextDouble() * 15 + 15, 1); break;
                    case "HUM": value = Math.Round(_rng.NextDouble() * 50 + 30, 1); break;
                    case "RUIDO": value = Math.Round(_rng.NextDouble() * 40 + 40, 1); break;
                    case "PM2.5": value = Math.Round(_rng.NextDouble() * 50 + 5, 1); break;
                    case "NO2": value = Math.Round(_rng.NextDouble() * 30 + 10, 1); break;
                    default: value = Math.Round(_rng.NextDouble() * 100, 1); break;
                }
                data.Add(type, value);
            }
            return data;
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