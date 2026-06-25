FROM ubuntu:24.04 AS builder
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libboost-system-dev \
    libboost-thread-dev \
    nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j$(nproc)

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y \
    libboost-system1.83.0 \
    libboost-thread1.83.0 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/build/socktest_backend .
EXPOSE 8080
ENV PORT=8080
ENV BIND_IP=0.0.0.0
CMD ["./socktest_backend"]
