FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY shortener ./shortener
RUN pip install --no-cache-dir ".[serve]"
RUN useradd --create-home appuser && mkdir /data && chown appuser /data
USER appuser
ENV DATA_DIR=/data SHARDS=4 NODE_ID=0
VOLUME /data
EXPOSE 8000
CMD ["uvicorn", "shortener.app:app", "--host", "0.0.0.0", "--port", "8000"]
