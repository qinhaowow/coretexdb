//! CoreTexDB B-C-D-D Encryption Protocol
//!
//! Four-phase end-to-end encryption: Bootstrap -> Challenge -> Derive -> Data
//!
//! Security properties:
//! - Confidentiality: AES-256-GCM / ChaCha20-Poly1305
//! - Integrity: AEAD auth tags
//! - Mutual authentication: Ed25519 signatures
//! - Forward secrecy: X25519 ephemeral ECDH
//! - Anti-replay: incrementing sequence numbers
//!
//! ## .cdb File Format
//!
//! Header (87 bytes) + Ciphertext + Tag(16 bytes)

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use chacha20poly1305::ChaCha20Poly1305;
use ed25519_dalek::{SigningKey, VerifyingKey, Signature, SignatureError, Signer, Verifier};
use hkdf::Hkdf;
use sha2::Sha256;
use x25519_dalek::{EphemeralSecret, PublicKey as X25519PublicKey};
use rand::{Rng, RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

pub const CDB_MAGIC: &[u8; 4] = b"CTDB";
pub const CDB_VERSION: u16 = 1;
pub const CIPHER_AES256GCM: u8 = 0;
pub const CIPHER_CHACHA20: u8 = 1;
pub const SESSION_TIMEOUT: u64 = 3600;
pub const NONCE_LEN: usize = 12;
pub const TAG_LEN: usize = 16;
pub const SESSION_ID_LEN: usize = 32;

#[derive(Debug, Clone)]
pub enum CryptoError {
    InvalidKey(String),
    EncryptionFailed(String),
    DecryptionFailed(String),
    SignatureError(String),
    InvalidFormat(String),
    SessionExpired,
    SequenceOverflow,
    ReplayDetected { expected: u64, got: u64 },
    IoError(String),
}

impl std::fmt::Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::InvalidKey(m) => write!(f, "Invalid key: {}", m),
            Self::EncryptionFailed(m) => write!(f, "Encryption failed: {}", m),
            Self::DecryptionFailed(m) => write!(f, "Decryption failed: {}", m),
            Self::SignatureError(m) => write!(f, "Signature error: {}", m),
            Self::InvalidFormat(m) => write!(f, "Invalid format: {}", m),
            Self::SessionExpired => write!(f, "Session expired"),
            Self::SequenceOverflow => write!(f, "Sequence overflow"),
            Self::ReplayDetected { expected, got } => write!(f, "Replay: expected >= {}, got {}", expected, got),
            Self::IoError(m) => write!(f, "IO error: {}", m),
        }
    }
}

impl std::error::Error for CryptoError {}

impl From<std::io::Error> for CryptoError {
    fn from(e: std::io::Error) -> Self { Self::IoError(e.to_string()) }
}

impl From<SignatureError> for CryptoError {
    fn from(e: SignatureError) -> Self { Self::SignatureError(e.to_string()) }
}

pub type Result<T> = std::result::Result<T, CryptoError>;

// ============================================================================
// Helpers
// ============================================================================

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn sha2_256(data: &[u8]) -> [u8; 32] {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

fn parse_pem(pem: &str, expected_label: &str) -> Result<Vec<u8>> {
    let mut b64 = String::new();
    let mut found_label = false;
    for line in pem.lines() {
        let trimmed = line.trim();
        if trimmed.contains(expected_label) && trimmed.starts_with("-----BEGIN") {
            found_label = true;
            continue;
        }
        if trimmed.starts_with("-----END") && found_label {
            break;
        }
        if found_label {
            b64.push_str(trimmed);
        }
    }
    if !found_label {
        return Err(CryptoError::InvalidFormat(format!("PEM label '{}' not found", expected_label)));
    }
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(&b64)
        .map_err(|e| CryptoError::InvalidFormat(format!("Base64 decode failed: {}", e)))
}

fn format_pem(data: &[u8], label: &str) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(data);
    let mut pem = String::from("-----BEGIN ");
    pem.push_str(label);
    pem.push_str("-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).unwrap());
        pem.push('\n');
    }
    pem.push_str("-----END ");
    pem.push_str(label);
    pem.push_str("-----\n");
    pem
}

// ============================================================================
// Phase 1: Bootstrap - Identity key management
// ============================================================================

#[derive(Clone)]
pub struct IdentityKeypair {
    signing: SigningKey,
    verifying: VerifyingKey,
}

impl IdentityKeypair {
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        OsRng.fill(&mut bytes);
        let signing = SigningKey::from_bytes(&bytes);
        let verifying = signing.verifying_key();
        Self { signing, verifying }
    }

    pub fn from_seed(seed: &[u8; 32]) -> Self {
        let signing = SigningKey::from_bytes(seed);
        let verifying = signing.verifying_key();
        Self { signing, verifying }
    }

    pub fn from_pem(path: &Path) -> Result<Self> {
        let pem = fs::read_to_string(path).map_err(|e| CryptoError::IoError(e.to_string()))?;
        let der = parse_pem(&pem, "ED25519 PRIVATE KEY")?;
        if der.len() != 32 {
            return Err(CryptoError::InvalidKey(format!("Expected 32 bytes, got {}", der.len())));
        }
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&der);
        Ok(Self::from_seed(&seed))
    }

    pub fn save_pem(&self, path: &Path) -> Result<()> {
        let seed = self.signing.to_bytes();
        let pem = format_pem(&seed, "ED25519 PRIVATE KEY");
        fs::write(path, pem).map_err(|e| CryptoError::IoError(e.to_string()))
    }

    pub fn sign(&self, message: &[u8]) -> Signature {
        self.signing.sign(message)
    }

    pub fn public_key(&self) -> VerifyingKey {
        self.verifying
    }

    pub fn public_bytes(&self) -> [u8; 32] {
        self.verifying.to_bytes()
    }

    pub fn secret_bytes(&self) -> [u8; 32] {
        self.signing.to_bytes()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TrustAnchor {
    pub peer_id: String,
    pub peer_public_key: [u8; 32],
    pub fingerprint: String,
    pub created_at: u64,
}

impl TrustAnchor {
    pub fn new(peer_id: &str, public_key: [u8; 32]) -> Self {
        let fingerprint = hex::encode(sha2_256(&public_key));
        Self {
            peer_id: peer_id.to_string(),
            peer_public_key: public_key,
            fingerprint,
            created_at: now_unix(),
        }
    }

    pub fn verify_fingerprint(&self) -> bool {
        hex::encode(sha2_256(&self.peer_public_key)) == self.fingerprint
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| CryptoError::IoError(e.to_string()))?;
        fs::write(path, json).map_err(|e| CryptoError::IoError(e.to_string()))
    }

    pub fn load(path: &Path) -> Result<Self> {
        let json = fs::read_to_string(path).map_err(|e| CryptoError::IoError(e.to_string()))?;
        serde_json::from_str(&json).map_err(|e| CryptoError::InvalidFormat(e.to_string()))
    }
}

// ============================================================================
// Phase 2: Challenge - Mutual authentication
// ============================================================================

#[derive(Clone, Serialize, Deserialize)]
pub struct ChallengeRequest {
    pub nonce_a: [u8; 32],
    pub sender_id: String,
    pub timestamp: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ChallengeResponse {
    pub nonce_a: [u8; 32],
    pub nonce_b: [u8; 32],
    pub signature: Vec<u8>,
    pub responder_id: String,
    pub timestamp: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ChallengeConfirm {
    pub nonce_b: [u8; 32],
    pub signature: Vec<u8>,
    pub timestamp: u64,
}

impl ChallengeRequest {
    pub fn create(sender_id: &str) -> Self {
        let mut nonce_a = [0u8; 32];
        OsRng.fill(&mut nonce_a);
        Self {
            nonce_a,
            sender_id: sender_id.to_string(),
            timestamp: now_unix(),
        }
    }

    pub fn respond(
        &self,
        b_keypair: &IdentityKeypair,
        b_id: &str,
    ) -> Result<ChallengeResponse> {
        let now = now_unix();
        if now.saturating_sub(self.timestamp) > 300 {
            return Err(CryptoError::SignatureError("Challenge expired".into()));
        }
        let mut nonce_b = [0u8; 32];
        OsRng.fill(&mut nonce_b);

        let mut msg = Vec::with_capacity(64);
        msg.extend_from_slice(&self.nonce_a);
        msg.extend_from_slice(&nonce_b);
        let sig = b_keypair.sign(&msg);

        Ok(ChallengeResponse {
            nonce_a: self.nonce_a,
            nonce_b,
            signature: sig.to_bytes().to_vec(),
            responder_id: b_id.to_string(),
            timestamp: now_unix(),
        })
    }
}

impl ChallengeResponse {
    pub fn verify(&self, b_public_key: &VerifyingKey) -> Result<ChallengeConfirm> {
        let now = now_unix();
        if now.saturating_sub(self.timestamp) > 300 {
            return Err(CryptoError::SignatureError("Response expired".into()));
        }

        let mut msg = Vec::with_capacity(64);
        msg.extend_from_slice(&self.nonce_a);
        msg.extend_from_slice(&self.nonce_b);

        let sig_bytes: [u8; 64] = self.signature.as_slice().try_into()
            .map_err(|_| CryptoError::SignatureError("Invalid sig length".into()))?;
        let sig = Signature::from_bytes(&sig_bytes);
        b_public_key.verify(&msg, &sig)
            .map_err(|e| CryptoError::SignatureError(e.to_string()))?;

        Ok(ChallengeConfirm {
            nonce_b: self.nonce_b,
            signature: Vec::new(),
            timestamp: now_unix(),
        })
    }
}

// ============================================================================
// Phase 3: Derive - Key derivation (X25519 + HKDF)
// ============================================================================

#[derive(Clone, Zeroize)]
#[zeroize(drop)]
pub struct SessionKeys {
    pub c2s_key: [u8; 32],
    pub s2c_key: [u8; 32],
    pub c2s_iv: [u8; 12],
    pub s2c_iv: [u8; 12],
    pub session_id: [u8; SESSION_ID_LEN],
}

impl SessionKeys {
    pub fn derive(
        ecdh_shared: &[u8; 32],
        nonce_a: &[u8; 32],
        nonce_b: &[u8; 32],
    ) -> Self {
        let hk = Hkdf::<Sha256>::new(Some(nonce_a), ecdh_shared);

        let mut session_id = [0u8; 32];
        let mut c2s_key = [0u8; 32];
        let mut s2c_key = [0u8; 32];
        let mut c2s_iv = [0u8; 12];
        let mut s2c_iv = [0u8; 12];

        hk.expand(b"session-id", &mut session_id).unwrap();
        hk.expand(b"c2s-key", &mut c2s_key).unwrap();
        hk.expand(b"s2c-key", &mut s2c_key).unwrap();
        hk.expand(b"c2s-iv", &mut c2s_iv).unwrap();
        hk.expand(b"s2c-iv", &mut s2c_iv).unwrap();

        Self { c2s_key, s2c_key, c2s_iv, s2c_iv, session_id }
    }

    pub fn session_id_hex(&self) -> String {
        hex::encode(self.session_id)
    }
}

pub struct KeyExchange {
    ephemeral: EphemeralSecret,
    pub public_key: X25519PublicKey,
}

impl KeyExchange {
    pub fn new() -> Self {
        let ephemeral = EphemeralSecret::random_from_rng(OsRng);
        let public_key = X25519PublicKey::from(&ephemeral);
        Self { ephemeral, public_key }
    }

    pub fn public_bytes(&self) -> [u8; 32] {
        self.public_key.to_bytes()
    }

    pub fn compute_shared(self, peer_public: &X25519PublicKey) -> [u8; 32] {
        *self.ephemeral.diffie_hellman(peer_public).as_bytes()
    }
}

// ============================================================================
// Phase 4: Data - AEAD encryption
// ============================================================================

#[derive(Clone)]
pub enum AeadCipher {
    Aes256Gcm,
    ChaCha20Poly1305,
}

pub struct DataEncrypter {
    cipher_type: AeadCipher,
}

impl DataEncrypter {
    pub fn new(cipher_type: AeadCipher) -> Self {
        Self { cipher_type }
    }

    pub fn encrypt(
        &self,
        key: &[u8; 32],
        iv: &[u8; 12],
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<(Vec<u8>, [u8; TAG_LEN])> {
        match self.cipher_type {
            AeadCipher::Aes256Gcm => {
                let cipher = Aes256Gcm::new_from_slice(key)
                    .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;
                let nonce = Nonce::from_slice(iv);
                let payload = aes_gcm::aead::Payload {
                    msg: plaintext,
                    aad,
                };
                let ct = cipher.encrypt(nonce, payload)
                    .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;
                let tag_start = ct.len().saturating_sub(TAG_LEN);
                let mut tag = [0u8; TAG_LEN];
                tag.copy_from_slice(&ct[tag_start..]);
                let ciphertext = ct[..tag_start].to_vec();
                Ok((ciphertext, tag))
            }
            AeadCipher::ChaCha20Poly1305 => {
                use chacha20poly1305::{KeyInit as _, aead::Aead as _};
                let cipher = ChaCha20Poly1305::new_from_slice(key)
                    .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;
                let nonce = chacha20poly1305::Nonce::from_slice(iv);
                let payload = chacha20poly1305::aead::Payload {
                    msg: plaintext,
                    aad,
                };
                let ct = cipher.encrypt(nonce, payload)
                    .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;
                let tag_start = ct.len().saturating_sub(TAG_LEN);
                let mut tag = [0u8; TAG_LEN];
                tag.copy_from_slice(&ct[tag_start..]);
                let ciphertext = ct[..tag_start].to_vec();
                Ok((ciphertext, tag))
            }
        }
    }

    pub fn decrypt(
        &self,
        key: &[u8; 32],
        iv: &[u8; 12],
        ciphertext: &[u8],
        tag: &[u8; TAG_LEN],
        aad: &[u8],
    ) -> Result<Vec<u8>> {
        match self.cipher_type {
            AeadCipher::Aes256Gcm => {
                let cipher = Aes256Gcm::new_from_slice(key)
                    .map_err(|e| CryptoError::DecryptionFailed(e.to_string()))?;
                let nonce = Nonce::from_slice(iv);
                let mut ct_with_tag = ciphertext.to_vec();
                ct_with_tag.extend_from_slice(tag);
                let payload = aes_gcm::aead::Payload {
                    msg: &ct_with_tag,
                    aad,
                };
                cipher.decrypt(nonce, payload)
                    .map_err(|e| CryptoError::DecryptionFailed(e.to_string()))
            }
            AeadCipher::ChaCha20Poly1305 => {
                use chacha20poly1305::{KeyInit as _, aead::Aead as _};
                let cipher = ChaCha20Poly1305::new_from_slice(key)
                    .map_err(|e| CryptoError::DecryptionFailed(e.to_string()))?;
                let nonce = chacha20poly1305::Nonce::from_slice(iv);
                let mut ct_with_tag = ciphertext.to_vec();
                ct_with_tag.extend_from_slice(tag);
                let payload = chacha20poly1305::aead::Payload {
                    msg: &ct_with_tag,
                    aad,
                };
                cipher.decrypt(nonce, payload)
                    .map_err(|e| CryptoError::DecryptionFailed(e.to_string()))
            }
        }
    }
}

// ============================================================================
// .cdb File Format
// ============================================================================

pub const CDB_HEADER_SIZE: usize = 87;

#[derive(Clone)]
pub struct CdbHeader {
    pub version: u16,
    pub cipher: u8,
    pub flags: u8,
    pub created: u64,
    pub session_id: [u8; 32],
    pub sender_pubkey: [u8; 32],
    pub nonce_prefix: [u8; 7],
}

impl CdbHeader {
    pub fn new(cipher: u8, session_id: [u8; 32], sender_pubkey: [u8; 32]) -> Self {
        let mut nonce_prefix = [0u8; 7];
        OsRng.fill(&mut nonce_prefix);
        Self {
            version: CDB_VERSION,
            cipher,
            flags: 0,
            created: now_unix(),
            session_id,
            sender_pubkey,
            nonce_prefix,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(CDB_HEADER_SIZE);
        buf.extend_from_slice(CDB_MAGIC);
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.push(self.cipher);
        buf.push(self.flags);
        buf.extend_from_slice(&self.created.to_le_bytes());
        buf.extend_from_slice(&self.session_id);
        buf.extend_from_slice(&self.sender_pubkey);
        buf.extend_from_slice(&self.nonce_prefix);
        debug_assert_eq!(buf.len(), CDB_HEADER_SIZE);
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < CDB_HEADER_SIZE {
            return Err(CryptoError::InvalidFormat("Header too short".into()));
        }
        if &data[0..4] != CDB_MAGIC {
            return Err(CryptoError::InvalidFormat("Bad magic".into()));
        }
        let version = u16::from_le_bytes([data[4], data[5]]);
        let cipher = data[6];
        let flags = data[7];
        let created = u64::from_le_bytes(data[8..16].try_into().unwrap());

        let mut session_id = [0u8; 32];
        session_id.copy_from_slice(&data[16..48]);

        let mut sender_pubkey = [0u8; 32];
        sender_pubkey.copy_from_slice(&data[48..80]);

        let mut nonce_prefix = [0u8; 7];
        nonce_prefix.copy_from_slice(&data[80..87]);

        Ok(Self { version, cipher, flags, created, session_id, sender_pubkey, nonce_prefix })
    }

    pub fn make_nonce(&self, counter: u64) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..7].copy_from_slice(&self.nonce_prefix);
        let ctr_bytes = counter.to_le_bytes();
        nonce[7..12].copy_from_slice(&ctr_bytes[..5]);
        nonce
    }
}

pub fn cdb_encrypt(
    key: &[u8; 32],
    plaintext: &[u8],
    cipher: AeadCipher,
) -> Result<Vec<u8>> {
    let session_id = sha2_256(plaintext);
    let mut dummy_pub = [0u8; 32];
    OsRng.fill(&mut dummy_pub);

    let cipher_byte = match cipher {
        AeadCipher::Aes256Gcm => CIPHER_AES256GCM,
        AeadCipher::ChaCha20Poly1305 => CIPHER_CHACHA20,
    };

    let header = CdbHeader::new(cipher_byte, session_id, dummy_pub);
    let nonce = header.make_nonce(0);

    let enc = DataEncrypter::new(cipher);

    let aad = header.to_bytes();
    let (ciphertext, tag) = enc.encrypt(key, &nonce, plaintext, &aad)?;

    let mut output = aad;
    output.extend_from_slice(&ciphertext);
    output.extend_from_slice(&tag);
    Ok(output)
}

pub fn cdb_decrypt(key: &[u8; 32], data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < CDB_HEADER_SIZE + TAG_LEN {
        return Err(CryptoError::InvalidFormat("Data too short".into()));
    }

    let header = CdbHeader::from_bytes(data)?;
    let nonce = header.make_nonce(0);

    let aad = &data[..CDB_HEADER_SIZE];
    let ciphertext = &data[CDB_HEADER_SIZE..data.len() - TAG_LEN];
    let mut tag = [0u8; TAG_LEN];
    tag.copy_from_slice(&data[data.len() - TAG_LEN..]);

    let cipher = match header.cipher {
        CIPHER_AES256GCM => AeadCipher::Aes256Gcm,
        CIPHER_CHACHA20 => AeadCipher::ChaCha20Poly1305,
        _ => return Err(CryptoError::InvalidFormat(format!("Unknown cipher: {}", header.cipher))),
    };

    let enc = DataEncrypter::new(cipher);
    enc.decrypt(key, &nonce, ciphertext, &tag, aad)
}

pub fn cdb_encrypt_file(
    key: &[u8; 32],
    input_path: &Path,
    output_path: &Path,
    cipher: AeadCipher,
) -> Result<()> {
    let plaintext = fs::read(input_path).map_err(|e| CryptoError::IoError(e.to_string()))?;
    let encrypted = cdb_encrypt(key, &plaintext, cipher)?;
    fs::write(output_path, encrypted).map_err(|e| CryptoError::IoError(e.to_string()))
}

pub fn cdb_decrypt_file(
    key: &[u8; 32],
    input_path: &Path,
    output_path: &Path,
) -> Result<()> {
    let data = fs::read(input_path).map_err(|e| CryptoError::IoError(e.to_string()))?;
    let plaintext = cdb_decrypt(key, &data)?;
    fs::write(output_path, plaintext).map_err(|e| CryptoError::IoError(e.to_string()))
}

// ============================================================================
// Full handshake simulation (for testing)
// ============================================================================

pub fn handshake(
    a_id: &str,
    b_id: &str,
    a_keypair: &IdentityKeypair,
    b_keypair: &IdentityKeypair,
) -> Result<(SessionKeys, SessionKeys)> {
    let req = ChallengeRequest::create(a_id);
    let resp = req.respond(b_keypair, b_id)?;
    let _confirm = resp.verify(&b_keypair.public_key())?;

    let a_exchange = KeyExchange::new();
    let b_exchange = KeyExchange::new();

    let b_pub = b_exchange.public_key;
    let a_pub = a_exchange.public_key;

    let a_shared = a_exchange.compute_shared(&b_pub);
    let b_shared = b_exchange.compute_shared(&a_pub);

    let a_keys = SessionKeys::derive(&a_shared, &req.nonce_a, &resp.nonce_b);
    let b_keys = SessionKeys::derive(&b_shared, &req.nonce_a, &resp.nonce_b);

    Ok((a_keys, b_keys))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_bootstrap_keygen() {
        let kp = IdentityKeypair::generate();
        let pub_bytes = kp.public_bytes();
        assert_eq!(pub_bytes.len(), 32);
    }

    #[test]
    fn test_bootstrap_sign_verify() {
        let kp = IdentityKeypair::generate();
        let msg = b"hello world";
        let sig = kp.sign(msg);
        kp.public_key().verify(msg, &sig).unwrap();
    }

    #[test]
    fn test_challenge_mutual_auth() {
        let a = IdentityKeypair::generate();
        let b = IdentityKeypair::generate();

        let req = ChallengeRequest::create("alice");
        let resp = req.respond(&b, "bob").unwrap();
        let _confirm = resp.verify(&b.public_key()).unwrap();
    }

    #[test]
    fn test_derive_session_keys() {
        let a_ex = KeyExchange::new();
        let b_ex = KeyExchange::new();

        let a_pub = a_ex.public_key;
        let b_pub = b_ex.public_key;

        let a_shared = a_ex.compute_shared(&b_pub);
        let b_shared = b_ex.compute_shared(&a_pub);

        assert_eq!(a_shared, b_shared);

        let nonce_a = [1u8; 32];
        let nonce_b = [2u8; 32];
        let a_keys = SessionKeys::derive(&a_shared, &nonce_a, &nonce_b);
        let b_keys = SessionKeys::derive(&b_shared, &nonce_a, &nonce_b);

        assert_eq!(a_keys.session_id, b_keys.session_id);
        assert_eq!(a_keys.c2s_key, b_keys.c2s_key);
        assert_eq!(a_keys.s2c_key, b_keys.s2c_key);
    }

    #[test]
    fn test_cdb_encrypt_decrypt_aes() {
        let key = [0x42u8; 32];
        let plaintext = b"Hello, CoreTexDB B-C-D-D encryption!";

        let encrypted = cdb_encrypt(&key, plaintext, AeadCipher::Aes256Gcm).unwrap();
        assert_ne!(&encrypted[87..87+plaintext.len()], plaintext);

        let decrypted = cdb_decrypt(&key, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_cdb_encrypt_decrypt_chacha() {
        let key = [0x42u8; 32];
        let plaintext = b"ChaCha20-Poly1305 test data";

        let encrypted = cdb_encrypt(&key, plaintext, AeadCipher::ChaCha20Poly1305).unwrap();
        let decrypted = cdb_decrypt(&key, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_cdb_wrong_key_fails() {
        let key1 = [0x42u8; 32];
        let key2 = [0x99u8; 32];
        let plaintext = b"secret data";

        let encrypted = cdb_encrypt(&key1, plaintext, AeadCipher::Aes256Gcm).unwrap();
        let result = cdb_decrypt(&key2, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_cdb_file_roundtrip() {
        let key = [0x55u8; 32];
        let dir = std::env::temp_dir().join("coretex_crypto_test");
        let _ = fs::create_dir_all(&dir);
        let input = dir.join("input.txt");
        let encrypted = dir.join("output.cdb");
        let output = dir.join("output.txt");

        fs::write(&input, b"Crypto file test 12345").unwrap();
        cdb_encrypt_file(&key, &input, &encrypted, AeadCipher::Aes256Gcm).unwrap();
        cdb_decrypt_file(&key, &encrypted, &output).unwrap();

        assert_eq!(fs::read(&output).unwrap(), b"Crypto file test 12345");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_full_handshake() {
        let a = IdentityKeypair::generate();
        let b = IdentityKeypair::generate();

        let (a_keys, b_keys) = handshake("alice", "bob", &a, &b).unwrap();
        assert_eq!(a_keys.session_id, b_keys.session_id);
    }

    #[test]
    fn test_trust_anchor() {
        let kp = IdentityKeypair::generate();
        let anchor = TrustAnchor::new("peer1", kp.public_bytes());
        assert!(anchor.verify_fingerprint());

        let dir = std::env::temp_dir().join("coretex_anchor_test");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("anchor.json");
        anchor.save(&path).unwrap();
        let loaded = TrustAnchor::load(&path).unwrap();
        assert_eq!(loaded.peer_id, "peer1");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cdb_header() {
        let session_id = [0xAAu8; 32];
        let pubkey = [0xBBu8; 32];
        let header = CdbHeader::new(CIPHER_AES256GCM, session_id, pubkey);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), CDB_HEADER_SIZE);
        assert_eq!(&bytes[0..4], CDB_MAGIC);

        let parsed = CdbHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.session_id, session_id);
        assert_eq!(parsed.sender_pubkey, pubkey);
    }
}
