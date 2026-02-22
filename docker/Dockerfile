FROM ubuntu:22.04
ARG SPRING_VERSION=1.2.2
ARG DEB_URL=https://github.com/AntelopeIO/spring/releases/download/v${SPRING_VERSION}/antelope-spring_${SPRING_VERSION}_amd64.deb

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    curl -fsSL -o /tmp/spring.deb "${DEB_URL}" && \
    apt-get install -y /tmp/spring.deb && \
    rm /tmp/spring.deb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

EXPOSE 8080 8888 9876
ENTRYPOINT ["nodeos"]
