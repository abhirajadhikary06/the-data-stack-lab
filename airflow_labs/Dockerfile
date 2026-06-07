FROM apache/airflow:2.10.2

USER root

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		build-essential \
		git \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /opt/airflow

COPY --chown=airflow:root dags ./dags
COPY --chown=airflow:root plugins ./plugins

