FROM ubuntu:20.04

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

WORKDIR /

ENTRYPOINT ["./build/petrimaps"]
