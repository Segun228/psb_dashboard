FROM grafana/grafana:latest


RUN mkdir -p /etc/grafana/provisioning/datasources

COPY datasource.yaml /etc/grafana/provisioning/datasources/


ENV GF_SERVER_HTTP_PORT=10000
ENV GF_SECURITY_ADMIN_USER=admin
ENV GF_SECURITY_ADMIN_PASSWORD=${ADMIN_PASSWORD}

EXPOSE 10000