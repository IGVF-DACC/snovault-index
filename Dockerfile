FROM python:3.9.9-slim-buster

ENV PYTHONDONTWRITEBYTECODE 1

ENV PYTHONUNBUFFERED 1

ENV VIRTUAL_ENV=/opt/venv

RUN useradd -u 1444 -m snoindex

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    git \
    graphviz \
    libjpeg-dev \
    libmagic-dev \
    libpq-dev \
    libsqlite3-dev \
    make \
    zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv $VIRTUAL_ENV

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN chown -R snoindex:snoindex $VIRTUAL_ENV

WORKDIR /igvfd

COPY --chown=snoindex:snoindex setup.cfg pyproject.toml ./

COPY --chown=snoindex:snoindex src/snoindex/__init__.py src/snoindex/__init__.py

USER snoindex

RUN python -m pip install --upgrade pip

RUN pip install -e .[test]

CMD ["pytest -rf"]