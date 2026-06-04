import grpc
from concurrent import futures
import time
import json

# Import generated protobuf files
import preprocessing_pb2
import preprocessing_pb2_grpc

class DataPreprocessorServicer(preprocessing_pb2_grpc.DataPreprocessorServicer):
    def ProcessData(self, request, context):
        is_valid = True
        
        sensor_id = ""
        zone = ""
        data_type = ""
        val = 0.0
        data_date = ""
        data_time = ""

        if request.payload_format == "CUSTOM":
            # Parse custom format: ID:X | ZONE:Y | TYPE:Z | VAL:W | DATA:D | HORA:H
            parts = request.raw_payload.split('|')
            for part in parts:
                trimmed = part.strip()
                if ":" in trimmed:
                    idx = trimmed.find(':')
                    key = trimmed[:idx].strip()
                    value = trimmed[idx+1:].strip()
                    
                    if key == "ID": sensor_id = value
                    elif key == "ZONE": zone = value
                    elif key == "TYPE": data_type = value
                    elif key == "VAL":
                        try:
                            val = float(value.replace(",", "."))
                        except ValueError:
                            val = 0.0
                    elif key == "DATA": data_date = value
                    elif key == "HORA": data_time = value
        elif request.payload_format == "JSON":
            try:
                data = json.loads(request.raw_payload)
                sensor_id = data.get("ID", "")
                zone = data.get("ZONE", "")
                data_type = data.get("TYPE", "")
                val = float(data.get("VAL", 0.0))
                data_date = data.get("DATA", "")
                data_time = data.get("HORA", "")
            except Exception:
                is_valid = False

        processed_value = val
        processed_date = data_date

        print(f"[PREPROCESSOR] Formato: {request.payload_format} -> Parsed: {data_type} {processed_value}")

        # Bounding Outlier Filtering
        if data_type == "TEMPERATURA":
            if processed_value < -50: processed_value = -50
            elif processed_value > 60: processed_value = 60
        elif data_type in ["UMIDADE", "HUMIDADE"]:
            if processed_value < 0: processed_value = 0
            elif processed_value > 100: processed_value = 100
        elif data_type == "PRESSAO":
            if processed_value < 800: processed_value = 800
            elif processed_value > 1200: processed_value = 1200
        elif data_type == "VENTO":
            if processed_value < 0: processed_value = 0
            elif processed_value > 200: processed_value = 200
        elif data_type == "PRECIPITACAO":
            if processed_value < 0: processed_value = 0
            elif processed_value > 500: processed_value = 500

        # Convert Months by extension to numerical if present
        months = {
            "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03",
            "abril": "04", "maio": "05", "junho": "06", "julho": "07",
            "agosto": "08", "setembro": "09", "outubro": "10",
            "novembro": "11", "dezembro": "12"
        }
        
        lower_date = processed_date.lower()
        for month_name, month_num in months.items():
            if month_name in lower_date:
                processed_date = lower_date.replace(f"/{month_name}/", f"-{month_num}-")
                processed_date = processed_date.replace(f" {month_name} ", f"-{month_num}-")
                processed_date = processed_date.replace(month_name, month_num)
                break

        return preprocessing_pb2.ProcessReply(
            is_valid=is_valid,
            sensor_id=sensor_id,
            data_type=data_type,
            processed_value=processed_value,
            processed_date=processed_date,
            data_time=data_time,
            zone=zone
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    preprocessing_pb2_grpc.add_DataPreprocessorServicer_to_server(DataPreprocessorServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("Pré-processamento gRPC Server iniciado na porta 50051...")
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
