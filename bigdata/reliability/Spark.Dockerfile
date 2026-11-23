FROM news-spark:4.2.0-kafka
USER root
# Match the dependency declared by bigdata/spark/Dockerfile; the cached image predates it.
RUN python3 -m pip install --no-cache-dir prometheus-client==0.22.1
