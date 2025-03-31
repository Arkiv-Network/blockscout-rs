FROM ghcr.io/blockscout/services-base:chef-0.1.68-rust-1.83-bullseye-protoc-25.2-openapi-2.19.0 as chef

FROM chef AS plan
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=plan /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Include proto common definitions (will be used in a `build-contexts` section)
COPY --from=proto . /proto
# Build application
COPY . .
RUN cargo build --release

FROM node:16-bullseye-slim
WORKDIR /usr/src/

RUN apt-get update && apt-get install -y chromium curl \
    && npm link sol2uml@2.1 --only=production

COPY --from=build /app/target/release/visualizer-server ./
ENTRYPOINT ["./visualizer-server"]
