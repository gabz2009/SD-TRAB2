import grpc
from concurrent import futures
import time

# Import generated protobuf files
import preprocessing_pb2
import preprocessing_pb2_grpc

class DataPreprocessorServicer(preprocessing_pb2_grpc.DataPreprocessorServicer):
    def ProcessData(self, request, context):
        is_valid = True
        processed_value = request.value
        processed_date = request.data_date

        # Bounding Outlier Filtering
        if request.data_type == "TEMPERATURA":
            if processed_value < -50: processed_value = -50
            elif processed_value > 60: processed_value = 60
        elif request.data_type in ["UMIDADE", "HUMIDADE"]:
            if processed_value < 0: processed_value = 0
            elif processed_value > 100: processed_value = 100
        elif request.data_type == "PRESSAO":
            if processed_value < 800: processed_value = 800
            elif processed_value > 1200: processed_value = 1200
        elif request.data_type == "VENTO":
            if processed_value < 0: processed_value = 0
            elif processed_value > 200: processed_value = 200
        elif request.data_type == "PRECIPITACAO":
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
                # Naive replacement for " de Janeiro de " or similar
                processed_date = lower_date.replace(f"/{month_name}/", f"-{month_num}-")
                processed_date = processed_date.replace(f" {month_name} ", f"-{month_num}-")
                processed_date = processed_date.replace(month_name, month_num)
                break

        return preprocessing_pb2.ProcessReply(
            is_valid=is_valid,
            processed_value=processed_value,
            processed_date=processed_date
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
