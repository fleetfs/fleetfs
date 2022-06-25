use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Mutex;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use core::time::Duration;
use rkyv::AlignedVec;

const TIMEOUT: u64 = 10;

pub struct TcpClient {
    server: SocketAddr,
    // TODO: should probably have a connection pool here
    connection: Mutex<Option<TcpStream>>,
}

impl TcpClient {
    pub fn new(server: SocketAddr) -> TcpClient {
        TcpClient {
            server,
            connection: Mutex::new(None),
        }
    }

    pub fn send_and_receive_length_prefixed(
        &self,
        data: &[u8],
        response: &mut AlignedVec,
    ) -> Result<(), std::io::Error> {
        let mut locked = self.connection.lock().expect("lock acquisition failed");
        if locked.is_none() {
            let stream = TcpStream::connect_timeout(&self.server, Duration::from_secs(TIMEOUT))?;
            stream
                .set_read_timeout(Some(Duration::from_secs(TIMEOUT)))
                .expect("Timeout cannot be zero");
            stream
                .set_write_timeout(Some(Duration::from_secs(TIMEOUT)))
                .expect("Timeout cannot be zero");
            locked.replace(stream);
        }

        let mut stream = locked.take().expect("connected stream is None");

        match stream.write_all(data) {
            Ok(_) => {}
            Err(_) => {
                // Retry once
                stream = TcpStream::connect_timeout(&self.server, Duration::from_secs(TIMEOUT))?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(TIMEOUT)))
                    .expect("Timeout cannot be zero");
                stream
                    .set_write_timeout(Some(Duration::from_secs(TIMEOUT)))
                    .expect("Timeout cannot be zero");
                stream.write_all(data)?;
            }
        }

        let data_size = stream.read_u32::<LittleEndian>()? as usize;
        // TODO: should be able to more efficiently resize this
        response.clear();
        response.reserve(data_size);
        response.extend_from_slice(&vec![0; data_size]);
        stream.read_exact(response)?;

        // If the connection is still working, store it back
        locked.replace(stream);

        return Ok(());
    }
}
