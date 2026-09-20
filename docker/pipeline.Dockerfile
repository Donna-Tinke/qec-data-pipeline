FROM python:3.12.10-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        curl \
        make \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY docker/requirements.txt /tmp/course-requirements.txt
RUN pip install --no-cache-dir -r /tmp/course-requirements.txt

WORKDIR /workspace
EXPOSE 8888

