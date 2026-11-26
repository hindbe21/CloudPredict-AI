FROM apache/spark:3.5.7

USER root
COPY requirements-ml.txt /tmp/requirements-ml.txt
RUN python3 -m pip install --no-cache-dir --only-binary=:all: -r /tmp/requirements-ml.txt \
    && python3 -c "import numpy, scipy; print(numpy.__version__, scipy.__version__)"

USER spark
