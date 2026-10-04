FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY simulate.py .

RUN useradd --system --no-create-home sim
USER sim

ENTRYPOINT ["python", "simulate.py"]