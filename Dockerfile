FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --no-install-recommends -y\
       ca-certificates  \
       make \
       cmake \
       xxd \
	   # careful, OpenSSL is not thread safe, you MUST use GnuTLS
       libcurl4-gnutls-dev \
	   default-jre \
	   libpng-dev \
	   libomp-dev \
	   g++

COPY CMakeLists.txt /
ADD cmake /cmake
ADD src /src
ADD web /web

RUN mkdir build && cd build && cmake .. && make -j8

FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /
RUN apt update &&\
    apt install -y --no-install-recommends \
      ca-certificates \
      xxd \
      libgomp1 \
      libpng-dev \
      libcurl4-gnutls-dev &&\
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/petrimaps /petrimaps

ENTRYPOINT ["/petrimaps"]
