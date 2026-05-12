using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Sensor
{
    class sensor
    {
        private static TcpClient _client;
        private static NetworkStream _stream;
        private static string _sensorId = "S101"; // Deve estar na lista do Gateway
        private static readonly string[] _supportedTypes = { "TEMP", "HUM" };
        private static readonly Random _rng = new Random();
        private static bool _connected = false;
        private static bool _registado = false;

        static async Task Main(string[] args)
        {
            Console.Title = $"SENSOR - {_sensorId}";

            await ConectarAoGateway();

            // Enviar registo e esperar por Res:Ok
            await RegistarNoGateway();

            // Iniciar threads de leitura e heartbeat após registo bem-sucedido
            _ = Task.Run(() => LerRespostas());
            _ = Task.Run(() => EnviarHeartbeat());

            // Loop Principal: Simulação de recolha de dados
            while (true)
            {
                if (_registado)
                {
                    var measurements = GerarDadosSensor();
                    foreach (var measurement in measurements)
                    {
                        string type = measurement.Key;
                        double value = measurement.Value;
                        string dataMsg = $"OP:DATA | ID:{_sensorId} | ZONE:ZONA CENTRO | TYPE:{type} | VAL:{value:F1} | TS:{DateTime.Now:s}";

                        EnviarMensagem(dataMsg);
                        string unit = type == "TEMP" ? "ºC" : "%";
                        Console.WriteLine($"[DADOS] Enviado: {type} = {value:F1}{unit}");
                    }
                }

                Thread.Sleep(5000); // Envia dados a cada 5 segundos
            }
        }

        private static async Task ConectarAoGateway()
        {
            while (!_connected)
            {
                try
                {
                    _client = new TcpClient("127.0.0.1", 6000);
                    _stream = _client.GetStream();
                    _connected = true;
                    Console.WriteLine($"[{_sensorId}] Ligado ao Gateway.");
                }
                catch
                {
                    Console.WriteLine($"[{_sensorId}] Gateway offline. A tentar novamente...");
                    await Task.Delay(3000);
                }
            }
        }

        private static async Task RegistarNoGateway()
        {
            try
            {
                string typesList = string.Join(",", _supportedTypes);
                string regMsg = $"OP:REG | ID:{_sensorId} | TYPES:[{typesList}] | ZONE:ZONA CENTRO";
                EnviarMensagem(regMsg);
                Console.WriteLine($"[REG] Registo enviado ao Gateway...");

                // Esperar pela resposta Res:Ok com timeout de 5 segundos
                int attempts = 0;
                while (!_registado && attempts < 50)
                {
                    await Task.Delay(100);
                    attempts++;
                }

                if (_registado)
                {
                    Console.WriteLine("[OK] Registo confirmado pelo Gateway!");
                }
                else
                {
                    Console.WriteLine("[ERRO] Gateway não respondeu ao registo. A tentar novamente...");
                    await Task.Delay(2000);
                    if (!_registado) Console.WriteLine($"[{_sensorId}] A aguardar Res:Ok...");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO] Falha ao registar: {ex.Message}");
            }
        }

        private static async Task EnviarHeartbeat()
        {
            while (true)
            {
                if (_registado)
                {
                    string hbMsg = $"OP:HB | ID:{_sensorId} | TS:{DateTime.Now:s}";
                    EnviarMensagem(hbMsg);
                    Console.ForegroundColor = ConsoleColor.DarkGray;
                    Console.WriteLine($"[HB] Heartbeat enviado.");
                    Console.ResetColor();
                }
                await Task.Delay(10000); // Heartbeat a cada 10 segundos
            }
        }

        private static IDictionary<string, double> GerarDadosSensor()
        {
            double temperatura = Math.Round(_rng.NextDouble() * 15 + 20, 1); // 20.0 a 35.0 ºC
            double humidade = Math.Round(_rng.NextDouble() * 60 + 20, 1); // 20.0 a 80.0 %

            return new Dictionary<string, double>
            {
                { "TEMP", temperatura },
                { "HUM", humidade }
            };
        }

        private static async Task LerRespostas()
        {
            byte[] buffer = new byte[1024];
            while (_connected)
            {
                try
                {
                    int bytesRead = await _stream.ReadAsync(buffer, 0, buffer.Length);
                    if (bytesRead == 0)
                    {
                        _connected = false;
                        _registado = false;
                        break;
                    }

                    string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"[RES] {response}");
                    Console.ResetColor();

                    if (response.Contains("Res:Ok"))
                    {
                        _registado = true;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO] Falha ao ler respostas: {ex.Message}");
                    _connected = false;
                    _registado = false;
                }
            }
        }

        private static void EnviarMensagem(string msg)
        {
            try
            {
                byte[] data = Encoding.UTF8.GetBytes(msg);
                _stream.Write(data, 0, data.Length);
            }
            catch
            {
                Console.WriteLine("[ERRO] Conexão perdida!");
                _connected = false;
                _registado = false;
                ConectarAoGateway().Wait();
            }
        }
    }
}