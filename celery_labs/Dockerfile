FROM python:3.10-slim

WORKDIR /celery

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash"]