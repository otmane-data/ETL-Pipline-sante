FROM apache/superset:latest

COPY superset_config.py /app/pythonpath/
ENV SUPERSET_CONFIG_PATH /app/pythonpath/superset_config.py

USER root
RUN pip install psycopg2-binary

USER superset