FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# 1. Arreglar repositorios de Debian Stretch (EOL) e instalar dependencias
RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y wget ant

# 2. Descargar e instalar Pig
ENV PIG_VERSION=0.17.0
RUN wget https://archive.apache.org/dist/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz \
    && tar -xzf pig-$PIG_VERSION.tar.gz -C /usr/local/ \
    && ln -s /usr/local/pig-$PIG_VERSION /usr/local/pig \
    && rm pig-$PIG_VERSION.tar.gz

# 3. COMPILAR PIGGYBANK.JAR (Paso crítico nuevo)
# Entramos a la carpeta del código fuente y ejecutamos 'ant' para crear el .jar
WORKDIR /usr/local/pig/contrib/piggybank/java
RUN ant

# 4. Configurar entorno
ENV PIG_HOME=/usr/local/pig
ENV PATH=$PATH:$PIG_HOME/bin
ENV PIG_CLASSPATH=/etc/hadoop/conf

WORKDIR /scripts