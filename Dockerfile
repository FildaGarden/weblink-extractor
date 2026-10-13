FROM python:3.11-slim

# Create a non-privileged system user for secure container execution
RUN useradd --create-home appuser

WORKDIR /data

# Copy application code into container
COPY extractor/ /app/extractor/

# Add /app to Python module search path
ENV PYTHONPATH=/app

USER appuser

# Set entrypoint so the container acts as a direct CLI tool
ENTRYPOINT ["python", "-m", "extractor"]
CMD ["--help"]
