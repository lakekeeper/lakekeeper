FROM python:3.12-slim
WORKDIR /app

RUN pip install uv==0.8.11
COPY producer/pyproject.toml pyproject.toml
RUN uv pip install --system --no-cache .

COPY producer/main.py main.py

ENV PYTHONUNBUFFERED=1
RUN useradd -u 10001 -r -s /usr/sbin/nologin appuser && chown -R appuser:appuser /app
USER appuser
ENTRYPOINT ["python", "-u", "main.py"]
