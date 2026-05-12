using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.IO;
using System.Windows;
using System.Windows.Media;

namespace Server
{
    public partial class server : Window
    {
        private TcpListener _server;
        private bool _isRunning;
        private Thread _listenerThread;

        // PONTO 4: Mutex para ficheiro no Servidor
        private static Mutex _serverLogMutex = new Mutex();
        private static string _logPath = "log_servidor.txt";

        public MainWindow()
        {
            InitializeComponent();
        }

        private void btnStart_Click(object sender, RoutedEventArgs e)
        {
            _server = new TcpListener(IPAddress.Any, 5000);
            _server.Start();
            _isRunning = true;

            LogToUI("Servidor online (Porta 5000)");

            // PONTO 4: Atendimento Concorrente com Thread principal
            _listenerThread = new Thread(AcceptClients);
            _listenerThread.IsBackground = true;
            _listenerThread.Start();
            AtualizarEstadoUI(true);
        }

        // Nova função síncrona para correr na Thread principal
        private void AcceptClients()
        {
            while (_isRunning)
            {
                try
                {
                    TcpClient client = _server.AcceptTcpClient();

                    // PONTO 4: Nova Thread para cada Gateway que se ligue
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

        private void HandleClientData(TcpClient client)
        {
            using (client)
            using (NetworkStream stream = client.GetStream())
            {
                byte[] buffer = new byte[1024];
                while (_isRunning)
                {
                    try
                    {
                        // Leitura síncrona
                        int bytesRead = stream.Read(buffer, 0, buffer.Length);
                        if (bytesRead == 0) break;

                        string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                        LogToUI($"Recebido do Gateway: {message}");
                        ArmazenarDadosPorTipo(message);

                        // Guardar num ficheiro para demonstrar o Mutex no Servidor
                        GuardarLogServidor($"[{DateTime.Now:HH:mm:ss}] {message}");
                    }
                    catch { break; }
                }
            }
        }

        // PONTO 4: Função com Mutex no Servidor
        private void GuardarLogServidor(string mensagem)
        {
            _serverLogMutex.WaitOne();
            try
            {
                File.AppendAllText(_logPath, mensagem + Environment.NewLine);
            }
            catch (Exception ex)
            {
                LogToUI($"Erro ao guardar log: {ex.Message}");
            }
            finally
            {
                _serverLogMutex.ReleaseMutex();
                AtualizarEstadoUI(false);
            }
        }

        private void LogToUI(string message)
        {
            Dispatcher.Invoke(() => {
                txtLog.AppendText($"[{DateTime.Now:HH:mm:ss}] {message}{Environment.NewLine}");
                txtLog.ScrollToEnd();
            });
        }

        private void btnStop_Click(object sender, RoutedEventArgs e)
        {
            _isRunning = false;
            _server.Stop();
            LogToUI("Servidor offline");
            AtualizarEstadoUI(false);
        }

        private void AtualizarEstadoUI(bool online)
        {
            Dispatcher.Invoke(() =>
            {
                if (online)
                {
                    lblStatus.Text = "Online";
                    lblStatus.Foreground = Brushes.Green;
                    
                }
                else
                {
                    lblStatus.Text = "Offline";
                    lblStatus.Foreground = Brushes.Red; 
                }
            });
        }

        private static void ArmazenarDadosPorTipo(string msg)
        {
            // Divide a mensagem em linhas (ignora linhas vazias)
            var linhas = msg.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var linha in linhas)
            {
                var campos = linha.Split('|', StringSplitOptions.TrimEntries);
                string tipo = null;
                List<string> partesFiltradas = new();

                foreach (var campo in campos)
                {
                    if (campo.StartsWith("TYPE:", StringComparison.OrdinalIgnoreCase))
                    {
                        tipo = campo.Substring(5).ToUpperInvariant();
                        continue; // Não incluir TYPE na linha final
                    }
                    partesFiltradas.Add(campo.Trim());
                }

                if (string.IsNullOrEmpty(tipo))
                    continue; // Ignora linhas sem TYPE

                string nomeFicheiro = $"{char.ToUpper(tipo[0])}{tipo[1..].ToLower()}.txt";
                string linhaFinal = string.Join(" | ", partesFiltradas);

                try
                {
                    File.AppendAllText(nomeFicheiro, linhaFinal + Environment.NewLine);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO] Ao guardar a mensagem: {ex.Message}");
                }
            }
        }
    }
}