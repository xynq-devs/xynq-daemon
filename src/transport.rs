use tokio::io::{
    AsyncRead,
    AsyncReadExt,
    AsyncWrite,
    AsyncWriteExt,
};

/// Maximum payload accepted in a single length-prefixed frame (16 MiB).
///
/// A bounded frame size prevents an untrusted peer from forcing a multi-gigabyte
/// allocation by sending an arbitrary four-byte length prefix.
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Write a length-prefixed frame.
///
/// Frames use a four-byte, big-endian length followed by the payload.
pub async fn send_bytes<S>(send: &mut S, buf: Vec<u8>) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    if buf.len() > MAX_FRAME_SIZE {
        anyhow::bail!("frame exceeds maximum size of {MAX_FRAME_SIZE} bytes");
    }

    let len = u32::try_from(buf.len())
        .map_err(|_| anyhow::anyhow!("frame exceeds maximum encodable length"))?
        .to_be_bytes();

    send.write_all(&len)
        .await
        .map_err(|err| anyhow::anyhow!("failed to write frame length: {err}"))?;

    send.write_all(&buf)
        .await
        .map_err(|err| anyhow::anyhow!("failed to write frame payload: {err}"))?;

    Ok(())
}

/// Read a length-prefixed frame.
///
/// The length is validated before allocating memory. The payload buffer is
/// resized before `read_exact`; reserving capacity alone leaves its length at
/// zero and causes the payload read to consume no bytes.
pub async fn recv_bytes<R>(recv: &mut R) -> anyhow::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0_u8; 4];

    recv.read_exact(&mut len_buf)
        .await
        .map_err(|err| anyhow::anyhow!("failed to read frame length: {err}"))?;

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        anyhow::bail!("received frame exceeds maximum size of {MAX_FRAME_SIZE} bytes");
    }

    let mut bytes = vec![0; len];

    recv.read_exact(&mut bytes)
        .await
        .map_err(|err| anyhow::anyhow!("failed to read frame payload: {err}"))?;

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{MAX_FRAME_SIZE, recv_bytes, send_bytes};
    use tokio::io::duplex;

    #[tokio::test]
    async fn round_trips_non_empty_frame() {
        let (mut writer, mut reader) = duplex(64);
        let payload = b"xynq frame payload".to_vec();
        let expected = payload.clone();

        let write = tokio::spawn(async move {
            send_bytes(&mut writer, payload).await.unwrap();
        });

        let received = recv_bytes(&mut reader).await.unwrap();
        write.await.unwrap();
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn round_trips_empty_frame() {
        let (mut writer, mut reader) = duplex(8);

        send_bytes(&mut writer, Vec::new()).await.unwrap();

        assert!(recv_bytes(&mut reader).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejects_truncated_payload() {
        let (mut writer, mut reader) = duplex(8);

        use tokio::io::AsyncWriteExt;
        writer.write_all(&[0, 0, 0, 3, b'a']).await.unwrap();
        drop(writer);

        assert!(recv_bytes(&mut reader).await.is_err());
    }

    #[tokio::test]
    async fn rejects_oversized_frame_before_allocating_payload() {
        let (mut writer, mut reader) = duplex(8);

        use tokio::io::AsyncWriteExt;
        let oversized_len = (MAX_FRAME_SIZE as u32 + 1).to_be_bytes();
        writer.write_all(&oversized_len).await.unwrap();

        assert!(recv_bytes(&mut reader).await.is_err());
    }
}
