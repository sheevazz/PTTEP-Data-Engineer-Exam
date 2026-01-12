FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y curl gnupg && \
    curl https://sdk.cloud.google.com | bash && \
    /root/google-cloud-sdk/install.sh

ENV PATH="/root/google-cloud-sdk/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src
COPY sa /app/sa

ENTRYPOINT ["python", "/app/src/task1_ingest.py"]
