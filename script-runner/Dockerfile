FROM alpine:latest

RUN apk add --no-cache curl jq
RUN apk add --no-cache python3 py3-pip py3-requests

RUN adduser -u 1000 -h /home/runner runner --disabled-password
USER runner
WORKDIR /home/runner

COPY --chown=runner *.py /home/runner
RUN chmod +x /home/runner/*.py
