# Usamos una imagen ligera de Python
FROM python:3.11-slim

# Definimos el directorio de trabajo
WORKDIR /app

# Instalamos las librerías necesarias (Capa de caché para velocidad)
RUN pip install --no-cache-dir kafka-python boto3

# Copiamos los archivos de la carpeta scripts al contenedor
# Asegúrate de que bridge.py esté en la carpeta /scripts de tu PC
COPY ./scripts/ /app/

# Forzamos a Python a que no use buffer para ver los logs en tiempo real
ENV PYTHONUNBUFFERED=1

# Ejecutamos el script
CMD ["python", "bridge.py"]