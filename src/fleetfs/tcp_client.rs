use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Mutex;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;

pub struct TcpClient {
    server: SocketAddr,
    // TODO: should probably have a connection pool here
    connection: Mutex<Option<TcpStream>>
}

impl TcpClient {
    pub fn new(server: SocketAddr) -> TcpClient {
        TcpClient {
            server,
            connection: Mutex::new(None)
        }
    }

    pub fn send_and_receive_length_prefixed(&self, data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        let mut locked = self.connection.lock().expect("lock acquisition failed");
        if locked.is_none() {
            let stream = TcpStream::connect(self.server)?;
            locked.replace(stream);
        }

        let mut stream = locked.take().expect("connected stream is None");

        match stream.write_all(data) {
            Ok(_) => {},
            Err(_) => {
                // Retry once
                stream = TcpStream::connect(self.server)?;
                stream.write_all(data)?;
            }
        }

        let data_size = stream.read_u32::<LittleEndian>()?;
        let mut buffer = vec![0; data_size as usize];
        stream.read_exact(&mut buffer)?;

        // If the connection is still working, store it back
        locked.replace(stream);

        return Ok(buffer);
    }
}
