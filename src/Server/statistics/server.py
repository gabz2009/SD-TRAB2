import grpc
from concurrent import futures
import time
import sqlite3
import os

# Import generated protobuf files
import statistics_pb2
import statistics_pb2_grpc

class StatisticsCalculatorServicer(statistics_pb2_grpc.StatisticsCalculatorServicer):
    def GetStatistics(self, request, context):
        db_path = os.path.join(os.path.dirname(__file__), "..", "servidor.db")
        
        total_records = 0
        entries = []
        alerts = []
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Create AnalysisResults table if not exists
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS AnalysisResults (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Timestamp TEXT,
                    SensorId TEXT,
                    Zone TEXT,
                    Alert TEXT
                )
            ''')
            
            query = "SELECT COUNT(*), AVG(DataValue), DataType FROM SensorData WHERE 1=1"
            params = []
            
            if request.sensor_id:
                query += " AND SensorId = ?"
                params.append(request.sensor_id)
            if request.zone:
                query += " AND Zone = ?"
                params.append(request.zone)
            if request.date:
                query += " AND Timestamp LIKE ?"
                params.append(f"{request.date}%")
                
            query += " GROUP BY DataType"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            for row in rows:
                count = row[0]
                avg_value = row[1] if row[1] is not None else 0.0
                data_type = row[2]
                
                total_records += count
                entries.append(statistics_pb2.StatisticEntry(data_type=data_type, count=count, average=avg_value))
                
                # Check for health risk or extreme patterns
                alert_msg = None
                if data_type in ["TEMPERATURA"] and (avg_value > 35 or avg_value < 0):
                    alert_msg = f"Risco: Temperatura média extrema ({avg_value:.1f}°C) detetada"
                elif data_type in ["PM2.5"] and avg_value > 25:
                    alert_msg = f"Risco Saúde: Níveis elevados de PM2.5 ({avg_value:.1f})"
                elif data_type in ["NO2"] and avg_value > 40:
                    alert_msg = f"Alerta Poluição: Níveis elevados de NO2 ({avg_value:.1f})"
                
                if alert_msg:
                    if request.sensor_id:
                        alert_msg += f" no Sensor {request.sensor_id}"
                    if request.zone:
                        alert_msg += f" na Zona {request.zone}"
                    alerts.append(alert_msg)
                    
            # Store analysis result in DB regardless of alerts
            final_alert_text = " | ".join(alerts) if alerts else "Sem alertas detetados"
            cursor.execute('''
                INSERT INTO AnalysisResults (Timestamp, SensorId, Zone, Alert)
                VALUES (?, ?, ?, ?)
            ''', (time.strftime("%Y-%m-%dT%H:%M:%S"), request.sensor_id, request.zone, final_alert_text))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[ERRO DB] {e}")

        return statistics_pb2.AnalysisReply(
            total_records=total_records,
            entries=entries,
            alerts=alerts
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    statistics_pb2_grpc.add_StatisticsCalculatorServicer_to_server(StatisticsCalculatorServicer(), server)
    server.add_insecure_port('[::]:50052')
    print("Estatísticas gRPC Server iniciado na porta 50052...")
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
