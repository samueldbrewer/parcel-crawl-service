FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY parcel_crawl_demo_v4.py parcel_lookup.py ./
COPY ./api ./api
COPY ./worker ./worker
COPY ./storage ./storage
ENV PYTHONPATH=/app
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
