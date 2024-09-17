# FROM quay.io/astronomer/astro-runtime:5.0.1
FROM apache/airflow:2.7.1

USER root

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    git \
    unzip \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    POETRY_VERSION=1.4.2 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

# Add Poetry to PATH
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

USER airflow

WORKDIR /opt/airflow

RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"
    # Install Poetry
# RUN curl -sSL https://install.python-poetry.org | python3 -

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-dev --no-root

# ENV PYTHONPATH = "${PYTHONPATH}:/opt/airflow/src"
# COPY --chown=airflow:airflow src/ ./src
COPY --chown=airflow:airflow dags/ ./dags



