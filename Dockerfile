FROM python:3.10-bullseye

COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app

COPY . /app

ENV DAGSTER_HOME=/app/dagster_home/

RUN mkdir -p $DAGSTER_HOME

RUN touch $DAGSTER_HOME/dagster.yaml

EXPOSE 3000

CMD ["bash", "-c", "dagit -w workspace.yaml -h 0.0.0.0 -p 3000 & dagster-daemon run"]