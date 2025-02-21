FROM debian:bullseye-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-dev \
    gcc \
    g++ \
    libpq-dev \
    curl \
    gnupg2 \
    ca-certificates \
    lsb-release \
    && apt-get clean

# Install pip for system Python
RUN apt-get install -y python3-pip

# Upgrade pip, setuptools, and wheel
RUN python3 -m pip install --upgrade pip setuptools wheel

# Install required Python packages
RUN python3 -m pip install pyspark psutil python-dotenv

COPY . /app

EXPOSE 4040 7077 8080

CMD ["python3", "app/main.py"]
