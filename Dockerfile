FROM 3.9-alpine

WORKDIR /app
COPY pip.conf /etc/pip.conf
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
ENTRYPOINT "python", "app.py"