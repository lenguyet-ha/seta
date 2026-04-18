FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1 \
	PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl \
	&& rm -rf /var/lib/apt/lists/*

# Build context should be the repository root to include both user-service and shared.
COPY user-service/requirements.txt /tmp/requirements.txt
COPY shared /tmp/shared

RUN pip install --upgrade pip \
	&& pip install -r /tmp/requirements.txt \
	&& pip install /tmp/shared

COPY user-service /app

RUN adduser --disabled-password --gecos "" appuser \
	&& chown -R appuser:appuser /app

USER appuser

EXPOSE 8002

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002"]
