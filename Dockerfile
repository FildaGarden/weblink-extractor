FROM python:3.11-slim

RUN useradd --create-home appuser

WORKDIR /data

COPY extractor/ /app/extractor/

ENV PYTHONPATH=/app

USER appuser

ENTRYPOINT ["python", "-m", "extractor"]

CMD ["--help"]
