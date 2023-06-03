FROM apache/spark-py:v3.3.2

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --quiet --no-cache-dir -r /tmp/requirements.txt

COPY spark_setup.sh /usr/local/bin/spark_setup.sh

# Install Toree on jupyter
RUN jupyter toree install --spark_home=$SPARK_HOME --interpreters=Scala,PySpark,SQL

# Install curl
RUN apt-get update && apt-get install -y curl

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
RUN source $HOME/.cargo/env

# Expose jupyter server ports and spark ports
EXPOSE 8088-8089:8088-8089
EXPOSE 4040-4041:4040-4041

ENTRYPOINT [ "bash", "/usr/local/bin/spark_setup.sh" ]