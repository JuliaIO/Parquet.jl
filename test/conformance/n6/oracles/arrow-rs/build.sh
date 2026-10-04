#!/bin/sh
set -eu

oracle_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
source_dir="$oracle_dir/metadata"
build_root="$oracle_dir/build"
output="$build_root/parquet-jl-n6-arrow-rs-metadata"
docker=${PARQUET_N6_DOCKER:-docker}
image_reference=parquet-jl-n5-oracles:n5d-canonical-a
image_id=sha256:04e56f6512080165bff7c9ae869b27ab20b14fd2dbb7fd7dc3827bc2a1a6568b
expected_binary=a26a0a29f99adde346800e85ee66544226d2105a040b40eb3536e8d629d8c301
expected_size=710064

if [ -L "$build_root" ] || \
        { [ -e "$build_root" ] && [ ! -d "$build_root" ]; }; then
    echo "Arrow Rust build root must be a real directory" >&2
    exit 1
fi
if [ -L "$output" ] || { [ -e "$output" ] && [ ! -f "$output" ]; }; then
    echo "Arrow Rust metadata output must be a regular non-link file" >&2
    exit 1
fi
for input in "$source_dir/Cargo.toml" "$source_dir/src/main.rs"; do
    if [ -L "$input" ] || [ ! -f "$input" ]; then
        echo "Arrow Rust metadata source must be a regular non-link file" >&2
        exit 1
    fi
done
case "$source_dir" in
    *[,:]*)
        echo "Arrow Rust metadata source path cannot be mounted safely" >&2
        exit 1
        ;;
esac
identity=$($docker image inspect --format \
    '{{.Id}}|{{.Architecture}}|{{.Os}}' "$image_reference")
if [ "$identity" != "$image_id|amd64|linux" ]; then
    echo "local Arrow Rust image identity differs" >&2
    exit 1
fi

mkdir -p "$build_root"
if [ -L "$build_root" ] || [ ! -d "$build_root" ]; then
    echo "Arrow Rust build root changed while it was created" >&2
    exit 1
fi
temporary=$(mktemp -d "$build_root/.build.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM
user_id=$(id -u)
group_id=$(id -g)
$docker run --rm --pull never --network none --platform linux/amd64 \
    --read-only --cap-drop ALL --security-opt no-new-privileges \
    --pids-limit 128 --memory 2g --cpus 2 \
    --user "$user_id:$group_id" \
    --tmpfs /tmp:rw,nosuid,nodev,noexec,size=256m \
    --mount "type=bind,source=$source_dir,target=/source,readonly" \
    --mount "type=bind,source=$temporary,target=/build" \
    --workdir /build --entrypoint /bin/sh "$image_id" -c '
        mkdir -p /build/src
        cp /source/Cargo.toml /build/Cargo.toml
        cp /source/src/main.rs /build/src/main.rs
        cp /opt/bootstrap/arrow-rs/Cargo.lock /build/Cargo.lock
        CARGO_TARGET_DIR=/build/target cargo build --release --offline --locked
        cp /build/target/release/parquet-jl-n5-arrow-rs-oracle \
            /build/parquet-jl-n6-arrow-rs-metadata
    '
candidate="$temporary/parquet-jl-n6-arrow-rs-metadata"
actual=$(shasum -a 256 "$candidate" | awk '{print $1}')
if [ "$actual" != "$expected_binary" ] || \
        [ "$(stat -f '%z' "$candidate")" != "$expected_size" ]; then
    echo "Arrow Rust metadata binary identity differs" >&2
    exit 1
fi
chmod 0555 "$candidate"
mv -f "$candidate" "$output"
printf 'Built the pinned Arrow Rust N6 metadata oracle.\n'
