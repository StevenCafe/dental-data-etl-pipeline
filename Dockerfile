FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install functions-framework
COPY . .
CMD ["functions-framework","--target=hello_gcs","--port=8080"]