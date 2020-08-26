FROM python:3.8-slim as python-base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=true \
    POETRY_VERSION=1.0.10 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    PYSETUP_PATH="/app/" \
    VENV_PATH="/app/.venv"

# # prepend Poetry and VEnv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# build image
FROM python-base as builder-image

RUN apt-get --yes update \
    && apt-get --yes --no-install-recommends install \
        curl \
        build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python

WORKDIR $PYSETUP_PATH
COPY poetry.lock pyproject.toml README.md LICENSE ./
ADD scenographer ./scenographer

RUN poetry install --no-dev

# runtime image
FROM python-base as final-image
COPY --from=builder-image $PYSETUP_PATH $PYSETUP_PATH
CMD ["scenographer"]
