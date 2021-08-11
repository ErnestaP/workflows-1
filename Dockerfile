FROM python:3.8

WORKDIR /opt/workflows

ENTRYPOINT ["poetry", "run"]
CMD ["shell"]

ENV PYTHONBUFFERED=0 \
    PATH="/root/.poetry/bin:${PATH}" \
    POETRY_VIRTUALENVS_CREATE=false

ARG POETRY_VERSION
ENV POETRY_VERSION="${POETRY_VERSION:-1.1.6}"
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py \
  | python - --version "${POETRY_VERSION}" \
 && poetry --version

COPY poetry.lock pyproject.toml ./
RUN poetry install --no-root

COPY workflows workflows/
COPY workflows_tests workflows_tests/
COPY workspace.yaml workspace.yaml

ARG VERSION
ENV VERSION="${VERSION}"

RUN poetry install
