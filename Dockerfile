FROM ubuntu:18.04

RUN apt update && apt install -y git build-essential autoconf curl cmake libfuse-dev pkg-config fuse fio

RUN echo 'user_allow_other' >> /etc/fuse.conf

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

RUN mkdir /code && cd /code && git clone https://github.com/google/flatbuffers && cd flatbuffers && git checkout v1.11.0 \
  && cmake -G "Unix Makefiles" && make flatc && cp flatc /bin/flatc

ADD . /code/fleetfs/

ENV PATH=/root/.cargo/bin:$PATH
RUN cd /code/fleetfs && cargo build --release && cp target/release/fleetfs /bin/fleetfs
