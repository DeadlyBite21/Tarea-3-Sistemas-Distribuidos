FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# 1. Arreglar repositorios de Debian Stretch (para que apt-get funcione)
RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list && \
    apt-get -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y wget ca-certificates

# 2. Descargar e instalar Pig 0.17.0
ENV PIG_VERSION=0.17.0
RUN wget https://archive.apache.org/dist/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz \
    && tar -xzf pig-$PIG_VERSION.tar.gz -C /usr/local/ \
    && ln -s /usr/local/pig-$PIG_VERSION /usr/local/pig \
    && rm pig-$PIG_VERSION.tar.gz

# 3. INSTALAR PIGGYBANK (SOLUCIÓN: DESCARGA DIRECTA)
# En lugar de compilar (que falla con Hadoop 3), descargamos el JAR oficial pre-compilado.
# Lo guardamos exactamente donde tu script espera encontrarlo.
RUN wget --no-check-certificate -O /usr/local/pig/contrib/piggybank/java/piggybank.jar https://repo1.maven.org/maven2/org/apache/pig/piggybank/0.17.0/piggybank-0.17.0.jar

# 4. Configurar entorno
ENV PIG_HOME=/usr/local/pig
ENV PATH=$PATH:$PIG_HOME/bin
ENV PIG_CLASSPATH=/etc/hadoop/conf

WORKDIR /scripts