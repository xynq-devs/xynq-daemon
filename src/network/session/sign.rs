use hmac::{
    Hmac,
    KeyInit,
    Mac,
};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn sign_handshake(secret: &[u8; 32], timestamp: i64, nonce: &[u8; 32]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC can take key of any size");

    mac.update(&timestamp.to_be_bytes());
    mac.update(nonce);

    mac.finalize().into_bytes().into()
}

pub fn verify_handshake(
    secret: &[u8; 32],
    timestamp: i64,
    nonce: [u8; 32],
    signature: &[u8; 32],
) -> bool {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC can take key of any size");

    mac.update(&timestamp.to_be_bytes());
    mac.update(&nonce);

    mac.verify_slice(signature).is_ok()
}
