use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::id::PeerId;

type HmacSha256 = Hmac<Sha256>;

/// Authenticate identity and handshake freshness data as one domain-separated payload.
pub fn sign_handshake(
    secret: &[u8; 32],
    peer_id: PeerId,
    peer_name: &str,
    timestamp: i64,
    nonce: &[u8; 32],
) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts keys of any size");
    mac.update(b"xynq-handshake-v1");
    mac.update(peer_id.as_bytes());
    mac.update(&(peer_name.len() as u64).to_be_bytes());
    mac.update(peer_name.as_bytes());
    mac.update(&timestamp.to_be_bytes());
    mac.update(nonce);
    mac.finalize().into_bytes().into()
}

pub fn verify_handshake(
    secret: &[u8; 32],
    peer_id: PeerId,
    peer_name: &str,
    timestamp: i64,
    nonce: &[u8; 32],
    signature: &[u8; 32],
) -> bool {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts keys of any size");
    mac.update(b"xynq-handshake-v1");
    mac.update(peer_id.as_bytes());
    mac.update(&(peer_name.len() as u64).to_be_bytes());
    mac.update(peer_name.as_bytes());
    mac.update(&timestamp.to_be_bytes());
    mac.update(nonce);
    mac.verify_slice(signature).is_ok()
}

#[cfg(test)]
mod tests {
    use super::{sign_handshake, verify_handshake};
    use crate::id::PeerId;

    #[test]
    fn signature_binds_peer_identity() {
        let secret = [7; 32];
        let peer_id = PeerId::default();
        let nonce = [3; 32];
        let signature = sign_handshake(&secret, peer_id, "device", 1_800_000_000, &nonce);

        assert!(verify_handshake(
            &secret, peer_id, "device", 1_800_000_000, &nonce, &signature
        ));
        assert!(!verify_handshake(
            &secret, PeerId::default(), "device", 1_800_000_000, &nonce, &signature
        ));
        assert!(!verify_handshake(
            &secret, peer_id, "renamed", 1_800_000_000, &nonce, &signature
        ));
        assert!(!verify_handshake(
            &secret, peer_id, "device", 1_800_000_001, &nonce, &signature
        ));
    }
}
