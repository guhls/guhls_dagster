FROM python:3.10-bullseye

RUN pip install dagit dagster

WORKDIR /app

COPY workspace.yaml .

RUN mkdir /dagster_home

EXPOSE 3000

RUN export DAGSTER_HOME=~/dagster_home

CMD ["bash", "-c", "dagit -w worskpace.yaml -h 0.0.0.0 -p 3000 & dagster-daemon run"]