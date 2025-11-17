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

# --- INICIO DE LA CORRECCIÓN ---
# 3. BYPASS MANUAL (USANDO LA RUTA FÍSICA)
# ¡CAMBIO CLAVE! Nos movemos al directorio físico, no al symlink.
WORKDIR /usr/local/pig-0.17.0

# 3a. Creamos el directorio 'ivy' (ruta relativa a nuestro WORKDIR)
RUN mkdir -p ivy/

# 3b. Descargamos ivy.jar en 'ivy/' (ruta relativa)
RUN wget -O ivy/ivy-2.2.0.jar https://repo.maven.apache.org/maven2/org/apache/ivy/ivy/2.2.0/ivy-2.2.0.jar

# 3c. Creamos el directorio de destino
RUN mkdir -p build/ivy/lib

# 3d. ¡EL TRUCO! (Ahora con rutas relativas al WORKDIR físico)
# Java encontrará 'ivysettings.xml' y 'ivy.xml' en el directorio actual.
RUN java -jar ivy/ivy-2.2.0.jar \
    -settings ivysettings.xml \
    -ivy ivy.xml \
    -retrieve "build/ivy/lib/[artifact].[ext]"
# --- FIN DE LA CORRECCIÓN ---

# 4. COMPILAR PIGGYBANK.JAR
# ¡CAMBIO! Nos movemos al directorio físico de piggybank
WORKDIR /usr/local/pig-0.17.0/contrib/piggybank/java
RUN ant

# 5. Configurar entorno (Sin cambios, el symlink /usr/local/pig es útil para el PATH)
ENV PIG_HOME=/usr/local/pig
ENV PATH=$PATH:$PIG_HOME/bin
ENV PIG_CLASSPATH=/etc/hadoop/conf

WORKDIR /scripts