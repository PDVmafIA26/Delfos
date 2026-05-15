FROM postgres:15

# Instalar Python y pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Instalar librerías Python
RUN pip3 install --no-cache-dir --break-system-packages \
    psycopg2-binary \
    requests \
    urllib3