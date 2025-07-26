FROM apache/airflow:2.9.0-python3.10

user root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    python3-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

USER airflow