FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scraper.py merge.py empty.py second_pass.py second_pass_ddg.py filter.py .

CMD ["python", "scraper.py", "--part", "1"]
