FROM python:3.11-slim

RUN pip install dbt-core dbt-postgres

COPY ./dbt_meds /dbt

COPY ./profiles.yml /dbt/profiles.yml

WORKDIR /dbt

