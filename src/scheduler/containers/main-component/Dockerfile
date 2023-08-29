FROM python:3

WORKDIR /src/

COPY src/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./main_scheduler.py" ]