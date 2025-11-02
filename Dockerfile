FROM grafana/grafana:latest


RUN mkdir -p /etc/grafana/provisioning/dashboards


COPY dashboard.json /etc/grafana/provisioning/dashboards/


COPY dashboard.yaml /etc/grafana/provisioning/dashboards/

ENV GF_SECURITY_ADMIN_USER=admin

ENV GF_SECURITY_ADMIN_PASSWORD=admin