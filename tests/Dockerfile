FROM python:3.9-slim

WORKDIR /app

COPY ./requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install pytest

COPY ../dags/ ./dags/
COPY ../modules/ ./modules/
COPY ../tests/ ./tests/

CMD ["python", "-m", "pytest", "./tests", "-v"]