FROM python:3.11-slim-bullseye

RUN pip install poetry && rm -rf /root/.cache/*

WORKDIR /app
COPY poetry.lock pyproject.toml /app/
RUN poetry config virtualenvs.create false \
    && poetry lock --check \
    && poetry install --no-interaction --no-ansi --no-cache

COPY . /app

RUN poetry install --only main

STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/python", "/usr/local/bin/kafka-earliest-timestamp-exporter"]
