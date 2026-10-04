ARG BASE_IMAGE=eclipse-temurin:11.0.28_6-jdk@sha256:ab2527b3c9b7c15bc88f60dec19b2aa39939a6e0045fb8f538eeecbd7af59c69
ARG TARGETPLATFORM=linux/amd64
ARG SOURCE_DATE_EPOCH=1787356800
FROM --platform=${TARGETPLATFORM} ${BASE_IMAGE}

ARG BASE_IMAGE
ARG SOURCE_DATE_EPOCH
ARG UBUNTU_SNAPSHOT=20260822T000000Z
ARG BUILD_ESSENTIAL_VERSION=12.10ubuntu1
ARG CA_CERTIFICATES_VERSION=20260601~24.04.1
ARG CMAKE_VERSION=3.28.3-1build7
ARG GIT_VERSION=1:2.43.0-1ubuntu7.3
ARG PKG_CONFIG_VERSION=1.8.1-2build1
ARG XZ_UTILS_VERSION=5.6.1+really5.4.5-1ubuntu0.3
ARG MAVEN_VERSION=3.9.8
ARG MAVEN_SHA512=7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2
ARG RUST_VERSION=1.96.1
ARG RUST_DIST_DATE=2026-06-30
ARG RUST_CHANNEL_SHA256=87eb76c53073e72b766083bed5530820694253b832a762d8385bda5759f03975
ARG RUST_TARBALL_SHA256=d29ccb1559a177c4e72291f6e5f629de7fe8885e7521ca47802627544b121e95
ARG PERL_PACKAGE_VERSION=5.38.2-3.2ubuntu0.3
ARG PERL_VERSION=v5.38.2
ARG JSON_PP_VERSION=4.16

ENV CARGO_HOME=/opt/n5/cargo \
    CARGO_NET_OFFLINE=true \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    MAVEN_OPTS=-Dmaven.repo.local=/opt/n5/maven/repository \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH} \
    TZ=UTC

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN set -eux; \
    snapshot="https://snapshot.ubuntu.com/ubuntu/${UBUNTU_SNAPSHOT}"; \
    sed -i \
        -e "s#http://archive.ubuntu.com/ubuntu/#${snapshot}/#" \
        -e "s#http://security.ubuntu.com/ubuntu/#${snapshot}/#" \
        /etc/apt/sources.list.d/ubuntu.sources; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        "build-essential=${BUILD_ESSENTIAL_VERSION}" \
        "ca-certificates=${CA_CERTIFICATES_VERSION}" \
        "cmake=${CMAKE_VERSION}" \
        "git=${GIT_VERSION}" \
        "perl=${PERL_PACKAGE_VERSION}" \
        "pkg-config=${PKG_CONFIG_VERSION}" \
        "xz-utils=${XZ_UTILS_VERSION}"; \
    test "$(dpkg-query -W -f='${Version}' build-essential)" = "$BUILD_ESSENTIAL_VERSION"; \
    test "$(dpkg-query -W -f='${Version}' ca-certificates)" = "$CA_CERTIFICATES_VERSION"; \
    test "$(dpkg-query -W -f='${Version}' cmake)" = "$CMAKE_VERSION"; \
    test "$(dpkg-query -W -f='${Version}' git)" = "$GIT_VERSION"; \
    test "$(dpkg-query -W -f='${Version}' perl)" = "$PERL_PACKAGE_VERSION"; \
    test "$(dpkg-query -W -f='${Version}' pkg-config)" = "$PKG_CONFIG_VERSION"; \
    test "$(dpkg-query -W -f='${Version}' xz-utils)" = "$XZ_UTILS_VERSION"; \
    rm -rf /var/cache/* /var/lib/apt/lists/*; \
    find /var/log -type f -delete

RUN set -eux; \
    archive="apache-maven-${MAVEN_VERSION}-bin.tar.gz"; \
    url="https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/${archive}"; \
    curl --fail --location --silent --show-error "$url" --output "/tmp/${archive}"; \
    printf '%s  %s\n' "$MAVEN_SHA512" "/tmp/${archive}" | sha512sum --check --strict; \
    tar --extract --gzip --file "/tmp/${archive}" --directory /opt; \
    ln --symbolic "/opt/apache-maven-${MAVEN_VERSION}/bin/mvn" /usr/local/bin/mvn; \
    rm "/tmp/${archive}"; \
    test "$(mvn --version | awk 'NR == 1 { print $3 }')" = "$MAVEN_VERSION"

RUN set -eux; \
    channel="channel-rust-${RUST_VERSION}.toml"; \
    channel_url="https://static.rust-lang.org/dist/${channel}"; \
    archive="rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.xz"; \
    archive_url="https://static.rust-lang.org/dist/${RUST_DIST_DATE}/${archive}"; \
    curl --fail --location --silent --show-error "$channel_url" --output "/tmp/${channel}"; \
    printf '%s  %s\n' "$RUST_CHANNEL_SHA256" "/tmp/${channel}" | sha256sum --check --strict; \
    curl --fail --location --silent --show-error "$archive_url" --output "/tmp/${archive}"; \
    printf '%s  %s\n' "$RUST_TARBALL_SHA256" "/tmp/${archive}" | sha256sum --check --strict; \
    tar --extract --xz --file "/tmp/${archive}" --directory /tmp; \
    "/tmp/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu/install.sh" \
        --prefix=/usr/local \
        --disable-ldconfig \
        --components=rustc,cargo,rust-std-x86_64-unknown-linux-gnu,rustfmt-preview; \
    install --directory /opt/n5/pins; \
    install --mode=0444 "/tmp/${channel}" /opt/n5/pins/; \
    rm -rf "/tmp/${archive}" "/tmp/${channel}" \
        "/tmp/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu"; \
    test "$(rustc --version)" = "rustc ${RUST_VERSION} (31fca3adb 2026-06-26)"; \
    test "$(cargo --version | sed 's/ (.*//')" = "cargo ${RUST_VERSION}"

COPY parquet-java /opt/bootstrap/parquet-java
COPY arrow-rs /opt/bootstrap/arrow-rs
COPY image/validate-image.sh /usr/local/bin/validate-n5-oracle-image

RUN set -eux; \
    chmod 0555 /usr/local/bin/validate-n5-oracle-image; \
    install --directory /opt/n5/maven/repository /opt/n5/manifests; \
    cd /opt/bootstrap/parquet-java; \
    mvn --batch-mode --no-transfer-progress clean test package; \
    mvn --batch-mode --no-transfer-progress --offline clean test package; \
    mvn --batch-mode --no-transfer-progress --offline dependency:tree \
        -DoutputFile=/opt/n5/manifests/maven-dependency-tree.txt \
        -DappendOutput=false; \
    rm -rf /opt/bootstrap/parquet-java/target; \
    find /opt/n5/maven/repository -type f \
        \( -name _remote.repositories -o -name resolver-status.properties \
            -o -name '*.lastUpdated' \) -delete; \
    rm -rf /tmp/*

RUN set -eux; \
    export CARGO_NET_OFFLINE=false; \
    cd /opt/bootstrap/arrow-rs; \
    cargo fetch --locked; \
    cargo vendor --locked --versioned-dirs /opt/n5/vendor > /opt/n5/cargo/config.toml; \
    export CARGO_NET_OFFLINE=true; \
    cargo test --offline --locked; \
    cargo build --release --offline --locked; \
    cargo tree --locked > /opt/n5/manifests/cargo-dependency-tree.txt; \
    install --mode=0555 target/release/parquet-jl-n5-arrow-rs-oracle \
        /usr/local/bin/parquet-jl-n5-arrow-rs-oracle; \
    rm -rf /opt/bootstrap/arrow-rs/target; \
    find /opt/n5/cargo -mindepth 1 -maxdepth 1 ! -name config.toml \
        -exec rm -rf -- '{}' +; \
    rm -rf /tmp/*

RUN set -eux; \
    test "$(dpkg-query -W -f='${Version}' perl)" = "$PERL_PACKAGE_VERSION"; \
    test "$(perl -e 'print $^V')" = "$PERL_VERSION"; \
    test "$(perl -MJSON::PP -e 'print $JSON::PP::VERSION')" = "$JSON_PP_VERSION"; \
    printf '%s\n' \
        "base_image=${BASE_IMAGE}" \
        "source_date_epoch=${SOURCE_DATE_EPOCH}" \
        "ubuntu_snapshot=${UBUNTU_SNAPSHOT}" \
        "ubuntu_build_essential=${BUILD_ESSENTIAL_VERSION}" \
        "ubuntu_ca_certificates=${CA_CERTIFICATES_VERSION}" \
        "ubuntu_cmake=${CMAKE_VERSION}" \
        "ubuntu_git=${GIT_VERSION}" \
        "ubuntu_pkg_config=${PKG_CONFIG_VERSION}" \
        "ubuntu_xz_utils=${XZ_UTILS_VERSION}" \
        "maven_version=${MAVEN_VERSION}" \
        "maven_archive_sha512=${MAVEN_SHA512}" \
        "rust_version=${RUST_VERSION}" \
        "rust_channel_manifest_sha256=${RUST_CHANNEL_SHA256}" \
        "rust_tarball_sha256=${RUST_TARBALL_SHA256}" \
        "perl_package_version=${PERL_PACKAGE_VERSION}" \
        "perl_version=${PERL_VERSION}" \
        "json_pp_version=${JSON_PP_VERSION}" \
        'parquet_java_version=1.17.1' \
        'parquet_java_commit=78a8d3230eb4769db93de5f2f2e18363c04cae81' \
        'arrow_rs_version=59.2.0' \
        'arrow_rs_commit=782e5a685501a9db6cc8e9a3b7cbff894940c47a' \
        > /opt/n5/manifests/toolchains.txt; \
    test -z "$(find /opt/n5/maven/repository /opt/n5/vendor /opt/n5/cargo \
        /opt/n5/pins /opt/bootstrap ! -type d ! -type f -print -quit)"; \
    cd /opt/n5/maven/repository; \
    find . -type f -print0 > /tmp/n5-maven-files; \
    LC_ALL=C sort -z /tmp/n5-maven-files -o /tmp/n5-maven-files; \
    xargs -0 sha256sum < /tmp/n5-maven-files \
        > /opt/n5/manifests/maven-artifacts.sha256; \
    cd /; \
    find opt/n5/vendor opt/n5/cargo opt/n5/pins -type f -print0 \
        > /tmp/n5-cargo-files; \
    printf '%s\000' usr/local/bin/parquet-jl-n5-arrow-rs-oracle \
        >> /tmp/n5-cargo-files; \
    LC_ALL=C sort -z /tmp/n5-cargo-files -o /tmp/n5-cargo-files; \
    xargs -0 sha256sum < /tmp/n5-cargo-files \
        > /opt/n5/manifests/cargo-vendor.sha256; \
    find opt/bootstrap -type f -print0 > /tmp/n5-source-files; \
    printf '%s\000' opt/n5/manifests/toolchains.txt \
        usr/local/bin/validate-n5-oracle-image >> /tmp/n5-source-files; \
    LC_ALL=C sort -z /tmp/n5-source-files -o /tmp/n5-source-files; \
    xargs -0 sha256sum < /tmp/n5-source-files \
        > /opt/n5/manifests/harness-source.sha256; \
    rm /tmp/n5-maven-files /tmp/n5-cargo-files /tmp/n5-source-files; \
    test -s /opt/n5/manifests/maven-artifacts.sha256; \
    test -s /opt/n5/manifests/cargo-vendor.sha256; \
    test -s /opt/n5/manifests/harness-source.sha256; \
    cd /opt/n5/maven/repository; \
    sha256sum --check --quiet --strict /opt/n5/manifests/maven-artifacts.sha256; \
    cd /; \
    sha256sum --check --quiet --strict /opt/n5/manifests/cargo-vendor.sha256; \
    sha256sum --check --quiet --strict /opt/n5/manifests/harness-source.sha256; \
    chmod 0444 /opt/n5/cargo/config.toml /opt/n5/manifests/*; \
    find /opt/n5 /opt/bootstrap -exec touch --no-dereference \
        --date="@${SOURCE_DATE_EPOCH}" '{}' +; \
    touch --no-dereference --date="@${SOURCE_DATE_EPOCH}" \
        /usr/local/bin/parquet-jl-n5-arrow-rs-oracle \
        /usr/local/bin/validate-n5-oracle-image

WORKDIR /work
ENTRYPOINT ["/usr/local/bin/validate-n5-oracle-image"]
