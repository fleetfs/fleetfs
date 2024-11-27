use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Mutex;

use byteorder::ReadBytesExt;
use byteorder::{ByteOrder, LittleEndian};
use core::time::Duration;
use rkyv::util::AlignedVec;

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

    pub fn send_and_receive(
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

        let mut send_buffer = vec![0; data.len() + 4];
        LittleEndian::write_u32(&mut send_buffer, data.len() as u32);
        // TODO: optimize out this copy
        send_buffer[4..].copy_from_slice(data);

        match stream.write_all(&send_buffer) {
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
                stream.write_all(&send_buffer)?;
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

        Ok(())
    }
}
