use tokio::io::{
    AsyncRead,
    AsyncReadExt,
    AsyncWrite,
    AsyncWriteExt,
};

// pub trait ToMpsc: Sized {
//     async fn send_to_mpsc(self, tx: &tokio::sync::mpsc::Sender<Self>) -> anyhow::Result<()>;
// }

// pub trait FromMpsc: Sized {
//     async fn recv_from_mpsc(rx: &mut tokio::sync::mpsc::Receiver<Self>) -> anyhow::Result<Self>;
// }

// pub trait FromIpc: Sized {
//     async fn recv_from_ipc(
//         recv: &mut interprocess::local_socket::tokio::RecvHalf,
//     ) -> anyhow::Result<Self>;
// }

// pub trait ToIpc: Sized {
//     async fn send_to_ipc(
//         self,
//         send: &mut interprocess::local_socket::tokio::SendHalf,
//     ) -> anyhow::Result<()>;
// }

// pub trait FromQuic: Sized {
//     async fn recv_from_quic(recv: &mut quinn::RecvStream) -> anyhow::Result<Self>;
// }

// pub trait ToQuic: Sized {
//     async fn send_to_quic(self, send: &mut quinn::SendStream) -> anyhow::Result<()>;
// }

#[inline(always)]
pub async fn send_bytes<S>(send: &mut S, buf: Vec<u8>) -> anyhow::Result<()>
where
    S: AsyncWrite + AsyncWriteExt + Unpin,
{
    let len = (buf.len() as u32).to_be_bytes();

    send.write_all(&len)
        .await
        .map_err(|_| anyhow::anyhow!("stream write length failed"))?;

    send.write_all(&buf)
        .await
        .map_err(|_| anyhow::anyhow!("stream write bytes failed"))?;

    Ok(())
}

#[inline(always)]
pub async fn recv_bytes<R>(recv: &mut R) -> anyhow::Result<Vec<u8>>
where
    R: AsyncRead + AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];

    recv.read_exact(&mut len_buf)
        .await
        .map_err(|_| anyhow::anyhow!("stream read length failed"))?;

    let len = u32::from_be_bytes(len_buf) as usize;

    let mut bytes = Vec::with_capacity(len);

    recv.read_exact(&mut bytes)
        .await
        .map_err(|_| anyhow::anyhow!("stream read bytes failed"))?;

    Ok(bytes)
}
