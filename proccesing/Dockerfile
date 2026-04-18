# Usamos una imagen ligera de Python
FROM python:3.11-slim

# Definimos el directorio de trabajo
WORKDIR /app

# Ponemos pymongo
RUN pip install --no-cache-dir kafka-python pymongo

# Copiamos los archivos de la carpeta scripts al contenedor
COPY ./scripts/ /app/

# Forzamos a Python a que no use buffer para ver los logs en tiempo real
ENV PYTHONUNBUFFERED=1

# Ejecutamos el script
CMD ["python", "bridge.py"]