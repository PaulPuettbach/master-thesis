FROM python:3

WORKDIR /

COPY /src/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "src/testing.py" ]