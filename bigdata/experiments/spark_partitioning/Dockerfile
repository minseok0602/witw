ARG PROJECT_SPARK_IMAGE=news-spark:4.2.0-kafka
FROM ${PROJECT_SPARK_IMAGE}
USER root
RUN python3 -m pip install --no-cache-dir numpy==1.26.4 scikit-learn==1.6.1 scipy==1.15.3 joblib==1.5.1 threadpoolctl==3.6.0 prometheus-client==0.22.1
ENV OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 BLIS_NUM_THREADS=1 PYTHONHASHSEED=0
USER spark
