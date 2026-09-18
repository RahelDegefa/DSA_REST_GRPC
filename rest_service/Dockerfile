FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ALL files (app.py, items_pb2.py, items_pb2_grpc.py, reliability.py)
COPY . .

EXPOSE 5000

CMD ["python", "app.py"]