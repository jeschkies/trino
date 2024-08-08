FROM golang

RUN apt-get update && apt-get install -y jq && \
  go install github.com/mingrammer/flog@latest
COPY --chmod=0755 enhance.sh /bin/enhance
COPY --chmod=0755 cmd.sh /bin/cmd
COPY tenants /etc/tenants

CMD ["cmd"]
