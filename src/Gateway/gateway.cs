using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Grpc.Net.Client;
using Gateway.Grpc;

namespace Gateway
{
    class GatewayApp
    {
        // Configuração de sensores
        public static Dictionary<string, string[]> SensorDataTypes = new Dictionary<string, string[]>();
        public static Dictionary<string, string> SensorStatus = new Dictionary<string, string>();
        public static Dictionary<string, string> SensorZone = new Dictionary<string, string>();
        public static string ConfigPath = File.Exists("sensores_config.csv") ? "sensores_config.csv" :
                                          File.Exists(Path.Combine("src", "Gateway", "sensores_config.csv")) ? Path.Combine("src", "Gateway", "sensores_config.csv") :
                                          Path.Combine("..", "Gateway", "sensores_config.csv");
        public static readonly object CsvLock = new object();

        static async Task Main(string[] args)
        {
            await Task.Yield(); // Fix CS1998
            Console.Title = "GERENCIADOR DE GATEWAYS";
            CarregarConfiguracaoSensores();

            Console.WriteLine("Insira o ID do Gateway e os tipos de dados a subscrever (Ex: 102 TEMPERATURA), ou 'sair' para terminar:");

            HashSet<string> activeGateways = new HashSet<string>();

            while (true)
            {
                string? input = Console.ReadLine()?.Trim();
                if (string.IsNullOrEmpty(input)) continue;
                if (input.ToLower() == "sair") break;

                var parts = input.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length == 2)
                {
                    string id = "GTW" + parts[0];
                    if (activeGateways.Contains(id))
                    {
                        Console.WriteLine($"[ERRO] O Gateway {id} já está a ser executado!");
                        continue;
                    }
                    
                    string[] types = parts[1].Split(',').Select(t => t.Trim().ToUpper()).ToArray();

                    activeGateways.Add(id);
                    GatewayInstance gtw = new GatewayInstance(id, types);
                    Thread t = new Thread(() => gtw.Run().GetAwaiter().GetResult());
                    t.IsBackground = true;
                    t.Start();
                    Console.WriteLine($"[OK] Gateway {id} criado e a executar numa Thread (Subscrições: {string.Join(",", types)}).");
                }
                else
                {
                    Console.WriteLine("[ERRO] Formato inválido. Use: <ID> <TIPO1,TIPO2> (Ex: 102 HUMIDADE)");
                }
            }
        }

        public static void CarregarConfiguracaoSensores()
        {
            if (!File.Exists(ConfigPath))
            {
                Console.WriteLine($"[AVISO] Ficheiro de configuração '{ConfigPath}' não encontrado.");
                return;
            }

            try
            {
                foreach (var linha in File.ReadLines(ConfigPath))
                {
                    if (string.IsNullOrWhiteSpace(linha) || linha.StartsWith("#")) continue;

                    var partes = linha.Split(':');
                    if (partes.Length >= 4)
                    {
                        string sensorId = partes[0].Trim();
                        string estado = partes[1].Trim();
                        string zona = partes[2].Trim();
                        string tiposRaw = partes[3].Trim().Trim('[', ']');
                        string[] tipos = tiposRaw.Split(',').Select(t => t.Trim()).ToArray();

                        SensorStatus[sensorId] = estado;
                        SensorZone[sensorId] = zona;
                        SensorDataTypes[sensorId] = tipos;
                    }
                }
                Console.WriteLine($"[OK] Configuração carregada com sucesso.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO] Falha ao carregar configuração: {ex.Message}");
            }
        }

        public static bool ValidarSensorAtivo(string sensorId)
        {
            if (SensorStatus.TryGetValue(sensorId, out var estado))
            {
                if (estado.Equals("ativo", StringComparison.OrdinalIgnoreCase)) return true;
                if (estado.Equals("manutenção", StringComparison.OrdinalIgnoreCase)) return false;
                if (estado.Equals("desativado", StringComparison.OrdinalIgnoreCase)) return false;
            }
            return false;
        }

        public static void AtualizarLastSyncNoCSV(string sensorId)
        {
            try
            {
                var linhas = File.ReadAllLines(ConfigPath).ToList();
                for (int i = 0; i < linhas.Count; i++)
                {
                    if (linhas[i].StartsWith(sensorId + ":"))
                    {
                        var partes = linhas[i].Split(new[] { ':' }, 5);
                        if (partes.Length == 5)
                        {
                            partes[4] = DateTime.Now.ToString("s"); // Formato ISO 8601 sem timezone para consistência
                            linhas[i] = string.Join(":", partes);
                            break;
                        }
                    }
                }
                lock (CsvLock)
                {
                    File.WriteAllLines(ConfigPath, linhas);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO] Falha ao atualizar last_sync: {ex.Message}");
            }
        }
    }

    class GatewayInstance
    {
        private string _gatewayId;
        private string[] _subscribedTypes;
        private TcpClient? _serverClient;
        private NetworkStream? _serverStream;
        private readonly Queue<string> _serverMessage = new Queue<string>();
        private readonly object _queueLock = new object();
        private Timer? _batchTimer;
        private IConnection? _connection;
        private IChannel? _channel;
        private const string EXCHANGE_NAME = "urban_sensors";
        private GrpcChannel? _grpcChannel;
        //stub para preprocessor
        private DataPreprocessor.DataPreprocessorClient? _preprocessorClient;

        public GatewayInstance(string id, string[] types)
        {
            _gatewayId = id;
            _subscribedTypes = types;
        }

        public async Task Run()
        {
            _grpcChannel = GrpcChannel.ForAddress("http://localhost:50051");
            //criar stub para preprocessor
            _preprocessorClient = new DataPreprocessor.DataPreprocessorClient(_grpcChannel);

            _ = Task.Run(ConectarServidor);
            await ConectarRabbitMQ();
            
            while (true)
            {
                Thread.Sleep(1000);
            }
        }

        private async Task ConectarServidor()
        {
            while (_serverClient == null || !_serverClient.Connected)
            {
                try
                {
                    _serverClient = new TcpClient("127.0.0.1", 5000);
                    _serverStream = _serverClient.GetStream();
                    Console.WriteLine($"[{_gatewayId}] Ligado ao Servidor Central com sucesso!");
                    _batchTimer = new Timer(_ => EnviarBatchParaServidor(), null, 0, 10000);
                }
                catch (SocketException)
                {
                    await Task.Delay(3000);
                }
            }
        }

        private async Task ConectarRabbitMQ()
        {
            try
            {
                var factory = new ConnectionFactory { HostName = "localhost" };
                _connection = await factory.CreateConnectionAsync();
                _channel = await _connection.CreateChannelAsync();

                await _channel.ExchangeDeclareAsync(exchange: EXCHANGE_NAME, type: ExchangeType.Topic);

                var queueDeclareResult = await _channel.QueueDeclareAsync(queue: $"{_gatewayId}_queue", durable: true, exclusive: false, autoDelete: false);
                string queueName = queueDeclareResult.QueueName;

                await _channel.QueueBindAsync(queue: queueName, exchange: EXCHANGE_NAME, routingKey: "sensor.registration");
                
                foreach (var type in _subscribedTypes)
                {
                    await _channel.QueueBindAsync(queue: queueName, exchange: EXCHANGE_NAME, routingKey: $"sensor.data.*.{type}");
                }

                Console.WriteLine($"[{_gatewayId}] Ligado ao RabbitMQ. À espera de mensagens...");

                var consumer = new AsyncEventingBasicConsumer(_channel);
                consumer.ReceivedAsync += async (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;

                    try
                    {
                        await ProcessDataAsync(message, routingKey);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERRO {_gatewayId}] {ex.Message}");
                    }

                    await Task.CompletedTask;
                };

                await _channel.BasicConsumeAsync(queue: queueName, autoAck: true, consumer: consumer);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO NO RABBITMQ {_gatewayId}] {ex.Message}");
            }
        }

        private async Task ProcessDataAsync(string data, string routingKey)
        {
            var fields = ParseMessageFields(data);

            if (routingKey == "sensor.registration" || data.Contains("OP:REG"))
            {
                string sensorId = fields.GetValueOrDefault("ID", string.Empty);
                string typesField = fields.GetValueOrDefault("TYPES", string.Empty).Trim('[', ']');
                string[] types = typesField.Split(',').Select(value => value.Trim()).ToArray();

                if (GatewayApp.ValidarSensorAtivo(sensorId))
                {
                    GatewayApp.SensorDataTypes[sensorId] = types;
                    GatewayApp.AtualizarLastSyncNoCSV(sensorId);
                }
            }
            else if (routingKey.StartsWith("sensor.data") || data.Contains("OP:DATA"))
            {
                string sensorId = fields.GetValueOrDefault("ID", string.Empty);
                string type = fields.GetValueOrDefault("TYPE", string.Empty);
                
                if (GatewayApp.SensorDataTypes.TryGetValue(sensorId, out var allowedTypes) && allowedTypes.Contains(type))
                {
                    if (_subscribedTypes.Contains(type))
                    {
                        if (_preprocessorClient != null)
                        {  
                            try
                            {
                                //chama o metodo ProcessData do preprocessor
                                var reply = await _preprocessorClient.ProcessDataAsync(new ProcessRequest
                                {
                                    RawPayload = data,
                                    PayloadFormat = "CUSTOM"
                                });

                                if (reply.IsValid)
                                {
                                    string msgParaServidor = $"GTW_ID:{_gatewayId} | ID:{reply.SensorId} | ZONE:{reply.Zone} | TYPE:{reply.DataType} | VAL:{reply.ProcessedValue:F1} | TS:{reply.ProcessedDate}T{reply.DataTime}";
                                    Console.WriteLine($"[{_gatewayId}] Dados do {reply.SensorId} ({reply.DataType}) consumidos e validados via gRPC (Valor: {reply.ProcessedValue:F1}).");
                                    EnviarParaServidor(msgParaServidor);
                                }
                                else
                                {
                                    Console.WriteLine($"[{_gatewayId}] Dados rejeitados pelo Pré-Processamento gRPC: {sensorId} ({type}) - Valor ou formato inválido.");
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{_gatewayId}] Erro ao comunicar com gRPC (Pré-processamento): {ex.Message}");
                            }
                        }
                    }
                }
                else
                {
                    Console.WriteLine($"[{_gatewayId}] Dados do {sensorId} ({type}) rejeitados (Sensor não configurado ou tipo não permitido).");
                }
            }
        }

        private Dictionary<string, string> ParseMessageFields(string data)
        {
            var fields = new Dictionary<string, string>();
            var parts = data.Split('|', StringSplitOptions.RemoveEmptyEntries);
            foreach (var part in parts)
            {
                var trimmed = part.Trim();
                int colonIndex = trimmed.IndexOf(':');
                if (colonIndex > 0)
                {
                    string key = trimmed.Substring(0, colonIndex).Trim();
                    string value = trimmed.Substring(colonIndex + 1).Trim();
                    fields[key] = value;
                }
            }
            return fields;
        }

        private void EnviarParaServidor(string msg)
        {
            lock (_queueLock)
            {
                _serverMessage.Enqueue(msg);
            }
        }

        private void EnviarBatchParaServidor()
        {
            List<string> msgs = new List<string>();
            lock (_queueLock)
            {
                while (_serverMessage.Count > 0) { msgs.Add(_serverMessage.Dequeue()); }
            }
            if (msgs.Count > 0 && _serverClient != null && _serverClient.Connected)
            {
                try
                {
                    string pacote = string.Join("\n", msgs);
                    byte[] data = Encoding.UTF8.GetBytes(pacote);
                    if (_serverStream != null)
                    {
                        _serverStream.Write(data, 0, data.Length);
                        Console.WriteLine($"[{_gatewayId}] BATCH de {msgs.Count} mensagens enviadas para o Servidor.");
                    }
                }
                catch
                {
                    lock (_queueLock)
                    {
                        foreach (var msg in msgs) _serverMessage.Enqueue(msg);
                    }
                }
            }
        }
    }
}