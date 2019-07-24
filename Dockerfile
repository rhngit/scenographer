FROM python:3.7-stretch

COPY requirements*.txt /tmp/

RUN apt-get --yes update && \
    # apt-get --yes install nmap redis-tools && \
    mkdir /app && \
    pip install --no-cache-dir --requirement /tmp/requirements.txt --requirement /tmp/requirements-dev.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=true PYTHONPATH=/app
COPY scenographer .

CMD yes
