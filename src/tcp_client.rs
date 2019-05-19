use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Mutex;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use core::time::Duration;

const TIMEOUT: u64 = 5;

// TODO: should not use this on the server, since it's blocking
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
        response: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let mut locked = self.connection.lock().expect("lock acquisition failed");
        if locked.is_none() {
            let stream = TcpStream::connect_timeout(&self.server, Duration::from_secs(TIMEOUT))?;
            stream
                .set_read_timeout(Some(Duration::from_secs(TIMEOUT)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(TIMEOUT)))
                .unwrap();
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
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(TIMEOUT)))
                    .unwrap();
                stream.write_all(data)?;
            }
        }

        let data_size = stream.read_u32::<LittleEndian>()?;
        response.resize(data_size as usize, 0);
        stream.read_exact(response)?;

        // If the connection is still working, store it back
        locked.replace(stream);

        return Ok(());
    }
}
