FROM ubuntu:20.04

ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y fuse fio flatbuffers-compiler &&\
  echo 'user_allow_other' >> /etc/fuse.conf

ADD . /code/fleetfs/

ENV PATH=/root/.cargo/bin:$PATH
RUN apt update && apt install -y curl libfuse-dev pkg-config git build-essential &&\
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain=1.42.0 &&\
  cd /code/fleetfs &&\
  cargo build --release && cp target/release/fleetfs /bin/fleetfs &&\
  cargo clean && rm -rf ~/.cargo/registry && rm -rf ~/.cargo/git &&\
  rustup self uninstall -y &&\
  apt remove -y curl libfuse-dev pkg-config git build-essential && apt autoremove -y

