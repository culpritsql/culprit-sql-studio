# Build context: culprit-amd64 and/or culprit-arm64, LICENSE, and THIRD-PARTY-LICENSES.txt.
# dev/smoke-container.py and .github/workflows/collector-publish.yml stage that minimal context without local state.
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
ARG TARGETARCH
ARG OCI_SOURCE=https://github.com/culpritsql/culprit-sql-studio
ARG OCI_VERSION=0.6.4
ARG OCI_REVISION=unknown
LABEL org.opencontainers.image.title="Culprit Collector" \
      org.opencontainers.image.description="Bounded, read-only database telemetry collector and pure diagnostic synthesizer" \
      org.opencontainers.image.source="${OCI_SOURCE}" \
      org.opencontainers.image.version="${OCI_VERSION}" \
      org.opencontainers.image.revision="${OCI_REVISION}" \
      org.opencontainers.image.licenses="MIT"
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -m 0700 -o 65532 -g 65532 /var/lib/culprit
COPY --chmod=0555 culprit-${TARGETARCH} /usr/local/bin/culprit
COPY --chmod=0444 LICENSE THIRD-PARTY-LICENSES.txt /usr/share/doc/culprit/
ENV CULPRIT_KEYCHAIN=off
WORKDIR /var/lib/culprit
USER 65532:65532
EXPOSE 9098
ENTRYPOINT ["/usr/local/bin/culprit"]
CMD ["collect", "service", "--help"]
