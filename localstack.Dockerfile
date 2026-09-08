# Pinned: `latest` (2026.x) exits with code 55 unless a paid auth token is set.
FROM localstack/localstack:4.14.0

# Copy init script to a path that is not overwritten by the base image at
# runtime (e.g. anonymous volume on /etc/localstack/init). The entrypoint
# wrapper installs it into ready.d when the container starts.
COPY localstack-init/init-aws.sh /opt/gate-init/init-aws.sh
RUN sed -i 's/\r$//' /opt/gate-init/init-aws.sh && chmod +x /opt/gate-init/init-aws.sh

# Entrypoint wrapper: install init script into ready.d, then run LocalStack.
# Strip CRLF so shebang is not interpreted as /bin/bash\r (Linux "no such file").
COPY localstack-init/gate-entrypoint.sh /usr/local/bin/gate-entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/gate-entrypoint.sh && chmod +x /usr/local/bin/gate-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/gate-entrypoint.sh"]
