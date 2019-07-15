FROM ubuntu:18.04

RUN apt update && apt install -y fuse fio &&\
  echo 'user_allow_other' >> /etc/fuse.conf &&\
  apt install -y git build-essential autoconf cmake && mkdir /code && \
  cd /code && git clone https://github.com/google/flatbuffers && cd flatbuffers && git checkout v1.11.0 && \
  cmake -G "Unix Makefiles" && make flatc && cp flatc /bin/flatc && \
  rm -rf /code/flatbuffers && apt remove -y git build-essential autoconf cmake && apt autoremove -y

ADD . /code/fleetfs/

ENV PATH=/root/.cargo/bin:$PATH
RUN apt update && apt install -y curl libfuse-dev pkg-config git &&\
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y &&\
  cd /code/fleetfs &&\
  cargo build --release && cp target/release/fleetfs /bin/fleetfs &&\
  cargo clean && rm -rf ~/.cargo/registry && rm -rf ~/.cargo/git &&\
  rustup self uninstall -y &&\
  apt remove -y curl libfuse-dev pkg-config git && apt autoremove -y

