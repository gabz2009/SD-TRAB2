using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Gateway
{
    class gateway
    {
        private static TcpListener _sensorListener;
        private static TcpClient _serverClient;
        private static NetworkStream _serverStream;

        // Configuração de sensores (lida do ficheiro CSV)
        private static Dictionary<string, string[]> _sensorDataTypes = new Dictionary<string, string[]>();
        private static Dictionary<string, string> _sensorStatus = new Dictionary<string, string>();
        private static Dictionary<string, string> _sensorZone = new Dictionary<string, string>();
        private static string _configPath = "sensores_config.csv";
        private static readonly object _csvLock = new object();

        // Envio de Mensagens em Pilha
        private static readonly Queue<string> _serverMessage = new Queue<string>();
        private static readonly object _queueLock = new object();
        private static Timer _BatchTimer;

        // O Main perde o 'async Task' para focar no uso clássico exigido
        static void Main(string[] args)
        {
            Console.Title = "GATEWAY";

            // Carregar configuração de sensores do CSV
            CarregarConfiguracaoSensores();

            while (_serverClient == null || !_serverClient.Connected)
            {
                try
                {
                    Console.WriteLine("A tentar ligar ao Servidor Central (Porta 5000)...");
                    _serverClient = new TcpClient("127.0.0.1", 5000);
                    _serverStream = _serverClient.GetStream();
                    Console.WriteLine("[OK] Ligado ao Servidor Central com sucesso!");
                    _BatchTimer = new Timer(_ => EnviarBatchParaServidor(), null, 0, 10000); //10 segundos
                }
                catch (SocketException)
                {
                    Console.WriteLine("[!] Servidor não responde. Reententando em 3 segundos...");
                    Thread.Sleep(3000); // Usamos Thread.Sleep em vez de Task.Delay
                }
            }
            

            try
            {
                _sensorListener = new TcpListener(IPAddress.Any, 6000);
                _sensorListener.Start();
                Console.WriteLine("[OK] Gateway à espera de sensores na porta 6000...");

                while (true)
                {
                    // Aceitação síncrona (A thread principal bloqueia aqui até chegar um sensor)
                    TcpClient sensor = _sensorListener.AcceptTcpClient();

                    // PONTO 4: Atendimento Concorrente usando Threads clássicas
                    Thread clientThread = new Thread(() => HandleSensor(sensor));
                    clientThread.IsBackground = true;
                    clientThread.Start();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO NO LISTENER] {ex.Message}");
            }
        }

        private static void HandleSensor(TcpClient sensor)
        {
            using (sensor)
            using (NetworkStream sensorStream = sensor.GetStream())
            {
                byte[] buffer = new byte[2048];
                while (true)
                {
                    try
                    {
                        int bytesRead = sensorStream.Read(buffer, 0, buffer.Length);
                        if (bytesRead == 0) break;

                        string rawData = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                        ProcessData(rawData, sensorStream);
                    }
                    catch(Exception ex) {
                        Console.WriteLine($"Falha ao processar os dados no Sensor {ex.Message}");
                        break;
                    }
                }
            }
        }

        private static void ProcessData(string data, NetworkStream sensorStream)
        {
            byte[] okRes = Encoding.UTF8.GetBytes("Res:Ok");
            var fields = ParseMessageFields(data);

            if (data.Contains("OP:REG"))
            {
                string sensorId = fields.GetValueOrDefault("ID", string.Empty);
                string typesField = fields.GetValueOrDefault("TYPES", string.Empty).Trim('[', ']');
                string zone = fields.GetValueOrDefault("ZONE", string.Empty);
                string[] types = typesField
                    .Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(value => value.Trim())
                    .Where(value => !string.IsNullOrEmpty(value))
                    .ToArray();

                // Validar no CSV de configuração
                if (ValidarSensorAtivo(sensorId))
                {
                    _sensorDataTypes[sensorId] = types;
                    Console.WriteLine($"[REG] Sensor '{sensorId}' registado com sucesso com tipos: [{string.Join(",", types)}].");
                    AtualizarLastSyncNoCSV(sensorId);
                }
                else
                {
                    Console.WriteLine($"[REG] Sensor '{sensorId}' não está ativo na configuração. Ignorado.");
                }

                sensorStream.Write(okRes, 0, okRes.Length);
            }
            else if (data.Contains("OP:DATA"))
            {
                string sensorId = fields.GetValueOrDefault("ID", string.Empty);
                string type = fields.GetValueOrDefault("TYPE", string.Empty);
                if (_sensorDataTypes.TryGetValue(sensorId, out var allowedTypes) && allowedTypes.Contains(type))
                {
                    string msgParaServidor = $"GTW_ID:GTW01 | {data.Replace("OP:DATA | ", "")}";
                    EnviarParaServidor(msgParaServidor);
                }
                else
                {
                    Console.WriteLine($"[ERRO] Dado inválido do sensor '{sensorId}' para tipo '{type}'. Ignorado.");
                }

                sensorStream.Write(okRes, 0, okRes.Length);
            }
        }

        private static void EnviarParaServidor(string msg)
        {
            lock (_queueLock)
            {
                _serverMessage.Enqueue(msg);
            }
        }

        private static void EnviarBatchParaServidor()
        {
            List<string> msgs = new List<string>();
            lock (_queueLock)
            {
                while (_serverMessage.Count > 0) { msgs.Add(_serverMessage.Dequeue()); }
            }
            if (msgs.Count > 0) 
            {
                    try
                    {
                        string pacote = string.Join("\n", msgs);
                        byte[] data = Encoding.UTF8.GetBytes(pacote);
                        _serverStream.Write(data, 0, data.Length);
                        Console.WriteLine($"[BATCH] {msgs.Count} mensagens enviadas para o servidor");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERRO] Falha ao enviar mensagem para o Servidor");
                        lock (_queueLock)
                        {
                            foreach (var msg in msgs)
                            {
                                _serverMessage.Enqueue(msg);
                            }
                        }
                    }
            }

        }
        private static void CarregarConfiguracaoSensores()
        {
            if (!File.Exists(_configPath))
            {
                Console.WriteLine($"[AVISO] Ficheiro de configuração '{_configPath}' não encontrado. Criando com exemplo...");
                CriarFicheiroConfiguracao();
                return;
            }

            try
            {
                foreach (var linha in File.ReadLines(_configPath))
                {
                    if (string.IsNullOrWhiteSpace(linha) || linha.StartsWith("#"))
                        continue;

                    var partes = linha.Split(':');
                    if (partes.Length >= 5)
                    {
                        string sensorId = partes[0].Trim();
                        string estado = partes[1].Trim();
                        string zona = partes[2].Trim();
                        string tiposRaw = partes[3].Trim().Trim('[', ']');
                        string lastSync = partes[4].Trim();

                        string[] tipos = tiposRaw
                            .Split(',', StringSplitOptions.RemoveEmptyEntries)
                            .Select(t => t.Trim())
                            .ToArray();

                        _sensorStatus[sensorId] = estado;
                        _sensorZone[sensorId] = zona;
                        _sensorDataTypes[sensorId] = tipos;

                        Console.WriteLine($"[CONF] Sensor {sensorId}: {estado} | Zona: {zona} | Tipos: [{string.Join(",", tipos)}]");
                    }
                }
                Console.WriteLine($"[OK] Configuração carregada com sucesso.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO] Falha ao carregar configuração: {ex.Message}");
            }
        }

        private static void CriarFicheiroConfiguracao()
        {
            string conteudo = @"
S101:ativo:ZONA_CENTRO:[TEMP,HUM,RUIDO]:2026-04-14T00:00:00
S102:ativo:ZONA_ESCOLAR:[PM2.5,TEMP]:2026-04-14T00:00:00";

            try
            {
                File.WriteAllText(_configPath, conteudo);
                Console.WriteLine($"[OK] Ficheiro de configuração criado: {_configPath}");
                // Recarregar após criar
                CarregarConfiguracaoSensores();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO] Falha ao criar ficheiro de configuração: {ex.Message}");
            }
        }

        private static bool ValidarSensorAtivo(string sensorId)
        {
            if (_sensorStatus.TryGetValue(sensorId, out var estado))
            {
                if (estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
                else if (estado.Equals("manutenção", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"[!] Sensor '{sensorId}' está em manutenção.");
                    return false;
                }
                else if (estado.Equals("desativado", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"[!] Sensor '{sensorId}' está desativado.");
                    return false;
                }
            }
            Console.WriteLine($"[!] Sensor '{sensorId}' não encontrado na configuração.");
            return false;
        }

        private static void AtualizarLastSyncNoCSV(string sensorId)
        {
            try
            {
                var linhas = File.ReadAllLines(_configPath).ToList();
                for (int i = 0; i < linhas.Count; i++)
                {
                    if (linhas[i].StartsWith(sensorId + ":"))
                    {
                        var partes = linhas[i].Split(':');
                        if (partes.Length >= 5)
                        {
                            partes[4] = DateTime.Now.ToString("o");
                            linhas[i] = string.Join(":", partes);
                            break;
                        }
                    }
                }
                lock (_csvLock)
                {
                    File.WriteAllLines(_configPath, linhas);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO] Falha ao atualizar last_sync: {ex.Message}");
            }
        }

        private static Dictionary<string, string> ParseMessageFields(string data)
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
    }
}