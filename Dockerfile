FROM grafana/grafana:latest

RUN mkdir -p /etc/grafana/provisioning/dashboards
RUN mkdir -p /etc/grafana/provisioning/datasources

# Копируем конфиг датасорсов
COPY datasource.yaml /etc/grafana/provisioning/datasources/

# Копируем дашборды
COPY dashboard.json /etc/grafana/provisioning/dashboards/
COPY dashboard.yaml /etc/grafana/provisioning/dashboards/

ENV GF_SERVER_HTTP_PORT=10000
ENV GF_SECURITY_ADMIN_USER=admin
ENV GF_SECURITY_ADMIN_PASSWORD=${ADMIN_PASSWORD}

EXPOSE 10000