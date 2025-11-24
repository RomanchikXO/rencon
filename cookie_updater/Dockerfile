FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

WORKDIR /app

COPY cookie_updater/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "cookie_updater/main.py"]
