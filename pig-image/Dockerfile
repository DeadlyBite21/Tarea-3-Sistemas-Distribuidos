# Usamos la misma base que los otros servicios
FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# --- FIX PARA REPOSITORIOS DEBIAN STRETCH (EOL) ---
# Como Debian Stretch es antiguo, cambiamos las fuentes a archive.debian.org
# y desactivamos la verificación de validez de fecha para evitar errores.
RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y wget
# --------------------------------------------------

# Descargamos e instalamos Apache Pig 0.17.0
ENV PIG_VERSION=0.17.0
RUN wget https://archive.apache.org/dist/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz \
    && tar -xzf pig-$PIG_VERSION.tar.gz -C /usr/local/ \
    && ln -s /usr/local/pig-$PIG_VERSION /usr/local/pig \
    && rm pig-$PIG_VERSION.tar.gz

# Configuramos las variables de entorno
ENV PIG_HOME=/usr/local/pig
ENV PATH=$PATH:$PIG_HOME/bin
ENV PIG_CLASSPATH=/etc/hadoop/conf

# Directorio de trabajo por defecto
WORKDIR /scripts