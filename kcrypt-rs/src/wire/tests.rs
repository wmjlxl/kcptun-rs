use super::*;
use crate::crypt::CryptEngine;

#[test]
fn test_encrypt_decrypt_roundtrip() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(0xDEADBEEF);

    let plaintext = b"hello kcptun wire format test!";
    let encrypted = cb.encrypt_cfb(plaintext, &crypt);

    // Decrypt
    let mut enc_copy = encrypted.to_vec();
    let decrypted = cb.decrypt_cfb(&mut enc_copy, &crypt);

    assert!(decrypted.is_some());
    let dec = decrypted.unwrap();
    assert_eq!(&dec[..], plaintext);
}

/// Regression: `copy_to_bytes` took the whole slab when `len == remaining()`,
/// so every packet dropped capacity and the next `reserve` malloc'd again.
#[test]
fn encrypt_cfb_retains_reusable_capacity() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(0xA110C);
    let data = vec![0xABu8; 1200];
    for i in 0..16 {
        let pkt = cb.encrypt_cfb(&data, &crypt);
        assert_eq!(pkt.len(), CRYPTO_HEADER_SIZE + data.len(), "pkt {i}");
        assert!(
            cb.enc_buf.capacity() > 0,
            "encrypt_cfb dropped reusable capacity at pkt {i}"
        );
    }
}

/// Capacity must be amortized across same-size packets, not regrown
/// every call (the old `copy_to_bytes` bug did exactly that).
#[test]
fn encrypt_cfb_amortizes_allocation_across_packets() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(0xA110C);
    let data = vec![0xABu8; 1200];
    let _warm = cb.encrypt_cfb(&data, &crypt);
    let mut grows = 0usize;
    let mut last_cap = cb.enc_buf.capacity();
    for _ in 0..32 {
        let _pkt = cb.encrypt_cfb(&data, &crypt);
        let cap = cb.enc_buf.capacity();
        if cap > last_cap {
            grows += 1;
            last_cap = cap;
        }
    }
    assert!(
        grows < 16,
        "capacity grew {grows}/32 packets — reserve is not amortized"
    );
}

#[test]
fn encrypt_cfb_batch_is_wire_compatible_and_roundtrips() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(0xB47C4);
    let packets: Vec<Bytes> = (0..8u8)
        .map(|i| Bytes::from(vec![i.wrapping_mul(17); 300 + i as usize]))
        .collect();

    let mut out = Vec::new();
    cb.encrypt_cfb_batch(&packets, &crypt, &mut out);
    assert_eq!(out.len(), packets.len());
    for (i, (wire, plain)) in out.iter().zip(&packets).enumerate() {
        assert_eq!(wire.len(), CRYPTO_HEADER_SIZE + plain.len(), "pkt {i}");
        let mut enc = wire.to_vec();
        let dec = cb
            .decrypt_cfb(&mut enc, &crypt)
            .unwrap_or_else(|| panic!("decrypt failed at pkt {i}"));
        assert_eq!(&dec[..], &plain[..], "pkt {i}");
    }

    // A second batch must reuse the slab (no per-packet regrow).
    let mut grows = 0usize;
    let mut last_cap = cb.enc_buf.capacity();
    for _ in 0..16 {
        out.clear();
        cb.encrypt_cfb_batch(&packets, &crypt, &mut out);
        let cap = cb.enc_buf.capacity();
        if cap > last_cap {
            grows += 1;
            last_cap = cap;
        }
    }
    assert!(grows < 8, "batch slab regrew {grows}/16 batches");
}

#[test]
fn packet_nonces_are_unique_and_not_shared_between_peers() {
    // salsa20 is the sharpest case: its 8-byte stream nonce *is* the
    // first half of the packet nonce, so a repeat is literal keystream
    // reuse. aes-128 (CFB) shows the chained-keystream effect.
    for method in ["aes-128", "salsa20"] {
        let (crypt, _) = CryptEngine::select(method, b"test-key-12345678");
        let mut cb = CryptoBuf::new(0xCAFEBABE);

        let data = b"test data for nonce";
        let pkt1 = cb.encrypt_cfb(data, &crypt);
        let pkt2 = cb.encrypt_cfb(data, &crypt);

        assert_ne!(&pkt1[..NONCE_SIZE], &pkt2[..NONCE_SIZE], "{method} nonce");
        // Identical plaintext must not yield identical ciphertext.
        assert_ne!(
            &pkt1[NONCE_SIZE..],
            &pkt2[NONCE_SIZE..],
            "{method} keystream reused within a session"
        );

        // Both ends of a session share the key and the session id; their
        // nonce streams must still be unrelated. The previous
        // counter-based nonce had both start at zero, so packet i of the
        // client and packet i of the server used one keystream.
        let mut peer = CryptoBuf::new(0xCAFEBABE);
        let peer_pkt = peer.encrypt_cfb(data, &crypt);
        assert_ne!(
            &peer_pkt[..NONCE_SIZE],
            &pkt1[..NONCE_SIZE],
            "{method} peer nonce"
        );
        assert_ne!(
            &peer_pkt[NONCE_SIZE..],
            &pkt1[NONCE_SIZE..],
            "{method} keystream shared between peers"
        );
    }
}

#[test]
fn should_cpu_block_thresholds() {
    let tokio = OffloadProfile::Tokio;
    let (none_crypt, _) = CryptEngine::select("none", b"");
    // Null cipher: never offload — "encrypt" is just pointer moves.
    assert!(!should_cpu_block_encrypt(
        false,
        false,
        7,
        100_000,
        &none_crypt,
        tokio
    ));
    assert!(!should_cpu_block_encrypt(
        false,
        false,
        999,
        999_999,
        &none_crypt,
        tokio
    ));
    // Heavy 8-byte CFB under Tokio: early offload.
    let (cast5_crypt, _) = CryptEngine::select("cast5", b"test-key-12345678");
    assert!(
        should_cpu_block_encrypt(true, false, 1, 1, &cast5_crypt, tokio),
        "tokio cast5 1 pkt should offload"
    );
    assert!(
        should_cpu_block_encrypt(true, false, 1, 100, &cast5_crypt, tokio),
        "tokio cast5 small pkt should offload"
    );
    assert!(
        should_cpu_block_encrypt(true, false, 1, 511, &cast5_crypt, tokio),
        "tokio cast5 511B should offload"
    );
    assert!(
        should_cpu_block_encrypt(true, false, 1, 512, &cast5_crypt, tokio),
        "tokio cast5 512B should offload"
    );
    assert!(
        should_cpu_block_encrypt(true, false, 0, 1000, &cast5_crypt, tokio),
        "tokio cast5 1KiB should offload by bytes"
    );

    let (tdes_crypt, _) = CryptEngine::select("3des", b"123456781234567812345678");
    assert!(
        should_cpu_block_encrypt(true, false, 1, 100, &tdes_crypt, tokio),
        "3des small should offload (tokio)"
    );

    let (bf_crypt, _) = CryptEngine::select("blowfish", b"test-key");
    assert!(
        should_cpu_block_encrypt(true, false, 1, 100, &bf_crypt, tokio),
        "blowfish small should offload (tokio)"
    );

    let (tea_crypt, _) = CryptEngine::select("tea", b"test-key");
    assert!(
        should_cpu_block_encrypt(true, false, 1, 100, &tea_crypt, tokio),
        "tea small should offload (tokio)"
    );

    let (xtea_crypt, _) = CryptEngine::select("xtea", b"test-key");
    assert!(
        should_cpu_block_encrypt(true, false, 1, 100, &xtea_crypt, tokio),
        "xtea small should offload (tokio)"
    );

    // Non-heavy CFB (AES, SM4, Twofish) keeps the original 4/4KiB threshold.
    let (aes_crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    assert!(!should_cpu_block_encrypt(
        true, false, 3, 100, &aes_crypt, tokio
    ));
    assert!(should_cpu_block_encrypt(
        true, false, 4, 100, &aes_crypt, tokio
    ));
    assert!(should_cpu_block_encrypt(
        true, false, 1, 4096, &aes_crypt, tokio
    ));

    let (sm4_crypt, _) = CryptEngine::select("sm4", b"test-key-12345678");
    assert!(!should_cpu_block_encrypt(
        true, false, 3, 100, &sm4_crypt, tokio
    ));
    assert!(should_cpu_block_encrypt(
        true, false, 4, 100, &sm4_crypt, tokio
    ));

    let (twofish_crypt, _) = CryptEngine::select("twofish", b"test-key-12345678");
    assert!(!should_cpu_block_encrypt(
        true,
        false,
        3,
        100,
        &twofish_crypt,
        tokio
    ));
    assert!(should_cpu_block_encrypt(
        true,
        false,
        4,
        100,
        &twofish_crypt,
        tokio
    ));
    // AEAD: offload only large batches (≥8 pkts or ≥8 KiB).
    assert!(!should_cpu_block_encrypt(
        false,
        true,
        4,
        0,
        &none_crypt,
        tokio
    ));
    assert!(!should_cpu_block_encrypt(
        false,
        true,
        7,
        8191,
        &none_crypt,
        tokio
    ));
    assert!(should_cpu_block_encrypt(
        false,
        true,
        8,
        0,
        &none_crypt,
        tokio
    ));
    assert!(should_cpu_block_encrypt(
        false,
        true,
        1,
        8192,
        &none_crypt,
        tokio
    ));
    let (aead_crypt, _) = CryptEngine::select("aes-128-gcm", b"0123456789abcdef");
    assert!(aead_crypt.is_aead());
    assert!(should_cpu_block_encrypt(
        false,
        true,
        999,
        999_999,
        &aead_crypt,
        tokio
    ));
    // Fast ciphers: xor/salsa20 offload large batches only.
    let (xor_crypt, _) = CryptEngine::select("xor", b"test-key");
    assert!(!should_cpu_block_encrypt(
        true, false, 7, 8191, &xor_crypt, tokio
    ));
    assert!(should_cpu_block_encrypt(
        true, false, 8, 100, &xor_crypt, tokio
    ));
    assert!(should_cpu_block_encrypt(
        true, false, 1, 8192, &xor_crypt, tokio
    ));
    let (salsa_crypt, _) = CryptEngine::select("salsa20", b"test-key");
    assert!(!should_cpu_block_encrypt(
        true,
        false,
        7,
        100,
        &salsa_crypt,
        tokio
    ));
    assert!(should_cpu_block_encrypt(
        true,
        false,
        8,
        100,
        &salsa_crypt,
        tokio
    ));
    // snappy compress threshold: ≥16 KiB
    assert!(!should_cpu_block_compress(16383));
    assert!(should_cpu_block_compress(16384));
    // inbound decrypt (tokio): null never; AEAD at 4KiB; CFB at 512B
    assert!(!should_cpu_block_decrypt(false, false, 65535, tokio));
    assert!(!should_cpu_block_decrypt(false, true, 4095, tokio));
    assert!(should_cpu_block_decrypt(false, true, 4096, tokio));
    assert!(!should_cpu_block_decrypt(true, false, 511, tokio));
    assert!(should_cpu_block_decrypt(true, false, 512, tokio));
}

#[test]
fn encrypt_batch_null_and_cfb() {
    let packets: Vec<Bytes> = vec![Bytes::from(&b"aaa"[..]), Bytes::from(&b"bbbb"[..])];
    let (crypt, _) = CryptEngine::select("null", b"key");
    let cb = parking_lot::Mutex::new(CryptoBuf::new(1));
    let out = encrypt_batch(packets, &crypt, &cb, false, true);
    assert_eq!(out.len(), 2);
    assert_eq!(&out[0][..], b"aaa");
    assert_eq!(&out[1][..], b"bbbb");

    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let packets: Vec<Bytes> = vec![Bytes::from(&b"hello wire"[..])];
    let mut cb = CryptoBuf::new(2);
    let cb_mu = parking_lot::Mutex::new(CryptoBuf::new(2));
    let out = encrypt_batch(packets, &crypt, &cb_mu, true, true);
    assert_eq!(out.len(), 1);
    assert!(out[0].len() > 10);
    let mut enc = out[0].to_vec();
    let dec = cb.decrypt_cfb(&mut enc, &crypt).unwrap();
    assert_eq!(&dec[..], b"hello wire");
}

#[test]
fn encrypt_batch_allow_parallel_false_still_correct() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    // ≥4 packets so the parallel branch would normally engage.
    let packets: Vec<Bytes> = (0..8).map(|i| Bytes::from(vec![i as u8; 64])).collect();
    let cb_mu = parking_lot::Mutex::new(CryptoBuf::new(99));
    let out = encrypt_batch(
        packets.clone(),
        &crypt,
        &cb_mu,
        true,
        false, // force serial (cpu_block worker path)
    );
    assert_eq!(out.len(), 8);
    let mut cb = CryptoBuf::new(99);
    for (i, pkt) in out.iter().enumerate() {
        let mut enc = pkt.to_vec();
        let dec = cb.decrypt_cfb(&mut enc, &crypt).unwrap();
        assert_eq!(&dec[..], &vec![i as u8; 64][..]);
    }
}

#[test]
fn encrypt_batch_parallel_crc_matches_serial() {
    // Parallel path (heavy ciphers, large batch): prepare → finalize_encrypt_packet.
    // xor/salsa/none must stay serial even with allow_parallel=true.
    let (xor, _) = CryptEngine::select("xor", b"test-key-12345678");
    assert!(!should_parallel_cfb_encrypt(&xor, true, 64, 64 * 1024));
    let (none, _) = CryptEngine::select("none", b"k");
    assert!(!should_parallel_cfb_encrypt(&none, true, 64, 64 * 1024));
    let (aes, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    assert!(!should_parallel_cfb_encrypt(&aes, true, 4, 400));
    assert!(should_parallel_cfb_encrypt(&aes, true, 8, 8 * 2048));
    let (tdes, _) = CryptEngine::select("3des", b"test-key-12345678-24bytes!");
    assert!(should_parallel_cfb_encrypt(&tdes, true, 8, 1000));

    for method in &["aes-128", "3des", "xor", "salsa20"] {
        let (crypt, _) = CryptEngine::select(method, b"test-key-12345678-24bytes!!");
        // 16 × 2KiB ≈ 32KiB — above AES parallel gate; xor still serial.
        let packets: Vec<Bytes> = (0..16).map(|i| Bytes::from(vec![i as u8; 2048])).collect();
        let total: usize = packets.iter().map(|p| p.len()).sum();
        let expect_par = should_parallel_cfb_encrypt(&crypt, true, packets.len(), total);

        let cb_mu = parking_lot::Mutex::new(CryptoBuf::new(0xC2C0_u64));
        let out = encrypt_batch(packets.clone(), &crypt, &cb_mu, true, true);
        assert_eq!(out.len(), 16, "{method} batch len");

        let mut cb = CryptoBuf::new(7);
        for (i, pkt) in out.iter().enumerate() {
            let mut enc = pkt.to_vec();
            let dec = cb
                .decrypt_cfb(&mut enc, &crypt)
                .unwrap_or_else(|| panic!("{method} pkt {i} CRC/decrypt failed"));
            assert_eq!(&dec[..], &vec![i as u8; 2048][..], "{method} pkt {i}");
        }

        // prepare + finalize must match encrypt_cfb wire semantics (CRC over plain).
        // Nonces are per-packet random, so compare by decrypting rather
        // than by byte-equality against a second serial encryption.
        let mut prep_cb = CryptoBuf::new(0xBEEF);
        let mut check_cb = CryptoBuf::new(0xBEEF);
        let plain = b"parallel-crc-wire-check-payload!!";
        let mut prepared = prep_cb.prepare_encrypt(plain);
        assert_eq!(&prepared[NONCE_SIZE..CRYPTO_HEADER_SIZE], &[0, 0, 0, 0]);
        let nonce_before = prepared[..NONCE_SIZE].to_vec();
        CryptoBuf::finalize_encrypt_packet(&mut prepared, &crypt);
        let mut finalized = prepared.to_vec();
        let dec = check_cb
            .decrypt_cfb(&mut finalized, &crypt)
            .unwrap_or_else(|| panic!("{method}: finalize CRC/decrypt failed"));
        assert_eq!(
            &dec[..],
            &plain[..],
            "{method}: finalize must produce encrypt_cfb wire semantics \
             (expect_par={expect_par})"
        );
        assert_eq!(
            &finalized[..NONCE_SIZE],
            &nonce_before[..],
            "{method}: finalize must not rewrite the nonce"
        );
    }
}

#[test]
fn encrypt_packet_uses_cfb_header_for_all_cfb_ciphers() {
    let (salsa, _) = CryptEngine::select("salsa20", b"test-key-12345678");
    let (xor, _) = CryptEngine::select("xor", b"test-key");
    let (aes, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(7);
    let plain = b"ack-payload-body";

    let s = cb.encrypt_packet(plain, &salsa);
    assert_eq!(
        s.len(),
        CRYPTO_HEADER_SIZE + plain.len(),
        "salsa must use CFB header"
    );
    // Roundtrip via decrypt_cfb
    let mut sbuf = s.to_vec();
    let dec = cb.decrypt_cfb(&mut sbuf, &salsa);
    assert!(dec.is_some(), "salsa decrypt_cfb");
    assert_eq!(&dec.unwrap()[..], plain);

    let x = cb.encrypt_packet(plain, &xor);
    assert_eq!(
        x.len(),
        CRYPTO_HEADER_SIZE + plain.len(),
        "xor must use CFB header"
    );

    let a = cb.encrypt_packet(plain, &aes);
    assert_eq!(
        a.len(),
        CRYPTO_HEADER_SIZE + plain.len(),
        "aes must use CFB header"
    );
}

#[test]
fn salsa20_roundtrip_via_encrypt_batch() {
    let (crypt, name) = CryptEngine::select("salsa20", b"test-key-12345678");
    assert_eq!(name, "salsa20");

    let cb = parking_lot::Mutex::new(CryptoBuf::new(42));
    let packets: Vec<Bytes> = (0..4).map(|i| Bytes::from(vec![i as u8; 128])).collect();
    let encrypted = encrypt_batch(packets.clone(), &crypt, &cb, true, false);
    assert_eq!(encrypted.len(), 4);

    // Each encrypted packet should have CRYPTO_HEADER_SIZE bytes of header (standard CFB)
    for (i, pkt) in encrypted.iter().enumerate() {
        assert!(
            pkt.len() >= CRYPTO_HEADER_SIZE,
            "packet {} too short ({} bytes)",
            i,
            pkt.len()
        );
    }

    // Decrypt via standard CFB path
    for (i, pkt) in encrypted.iter().enumerate() {
        let mut buf = pkt.to_vec();
        let body = decrypt_cfb_in_place(&mut buf, &crypt, false);
        assert!(body.is_ok(), "packet {} decrypt_cfb_in_place", i);
        assert_eq!(body.unwrap(), &packets[i][..], "packet {} mismatch", i);
    }
}

#[test]
fn encrypt_batch_aead_via_engine() {
    let (crypt, name) = CryptEngine::select("aes-128-gcm", b"0123456789abcdef");
    assert_eq!(name, "aes-128-gcm");
    assert!(crypt.is_aead());
    let packets: Vec<Bytes> = vec![Bytes::from(&b"aead body"[..])];
    let cb = parking_lot::Mutex::new(CryptoBuf::new(3));
    // has_encryption ignored when engine is AEAD
    let out = encrypt_batch(packets, &crypt, &cb, false, true);
    assert_eq!(out.len(), 1);
    let plain = crypt.as_aead().unwrap().open(&out[0]).unwrap();
    assert_eq!(&plain[..], b"aead body");
}

#[test]
fn test_crc_mismatch_returns_none() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(0xDEAD);

    let plaintext = b"hello";
    let mut encrypted = cb.encrypt_cfb(plaintext, &crypt).to_vec();

    // Corrupt the CRC field (bytes 16..20)
    encrypted[17] ^= 0xFF;

    let result = cb.decrypt_cfb(&mut encrypted, &crypt);
    assert!(result.is_none());
}

#[test]
fn test_short_data_returns_none() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(0);

    let mut short = [0u8; 10]; // < CRYPTO_HEADER_SIZE (20)
    let result = cb.decrypt_cfb(&mut short, &crypt);
    assert!(result.is_none());
}

#[test]
fn test_buffer_reuse_no_reallocation() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(0);

    // Encrypt many packets of varying sizes
    for i in 0..100 {
        let data = vec![i as u8; 100 + i * 10];
        let encrypted = cb.encrypt_cfb(&data, &crypt);
        assert_eq!(encrypted.len(), CRYPTO_HEADER_SIZE + data.len());

        // Verify roundtrip
        let mut enc_copy = encrypted.to_vec();
        let decrypted = cb.decrypt_cfb(&mut enc_copy, &crypt);
        assert!(decrypted.is_some());
        assert_eq!(&decrypted.unwrap()[..], &data[..]);
    }
}

#[test]
fn test_none_crypt() {
    let (crypt, _) = CryptEngine::select("none", b"");
    let mut cb = CryptoBuf::new(0);

    let plaintext = b"test none cipher";
    let encrypted = cb.encrypt_cfb(plaintext, &crypt);
    // With none cipher, nonce and CRC are still written, data is not encrypted
    assert_eq!(&encrypted[CRYPTO_HEADER_SIZE..], plaintext);
}

#[test]
fn decrypt_cfb_in_place_roundtrip() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(0xBEEF);
    let plaintext = b"in-place body payload xyz";
    let encrypted = cb.encrypt_cfb(plaintext, &crypt);
    let mut buf = encrypted.to_vec();
    let body = decrypt_cfb_in_place(&mut buf, &crypt, false).unwrap();
    assert_eq!(body, plaintext);
    // body is a subslice of buf (after header)
    assert_eq!(body.as_ptr(), buf[CRYPTO_HEADER_SIZE..].as_ptr());
}

#[test]
fn decrypt_cfb_in_place_crc_and_short() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let mut cb = CryptoBuf::new(1);
    let mut enc = cb.encrypt_cfb(b"hello", &crypt).to_vec();
    enc[17] ^= 0xFF;
    assert_eq!(
        decrypt_cfb_in_place(&mut enc, &crypt, false),
        Err(InboundCryptError::CrcMismatch)
    );
    let mut short = [0u8; 10];
    assert_eq!(
        decrypt_cfb_in_place(&mut short, &crypt, false),
        Err(InboundCryptError::Short)
    );
}

#[test]
fn strip_probe_raw_kcp_cmd_keeps_buffer() {
    // Synthetic: decrypted buffer whose byte 4 is cmd PUSH (0x51) → no header.
    let mut buf = vec![0u8; 30];
    buf[4] = 0x51;
    let body = strip_cfb_header_if_present(&buf, true).unwrap();
    assert_eq!(body.len(), 30);
    assert_eq!(body.as_ptr(), buf.as_ptr());
}

#[test]
fn inbound_null_identity() {
    let b = b"raw datagram";
    assert_eq!(inbound_null(b), b);
}

/// A reused output `Vec` must be big enough for the whole batch before the
/// pushes start. `clear()` leaves `len == 0`, so a reserve argument computed
/// from `capacity()` under-reserves: with cap 8 and 9 packets the old
/// expression asked for exactly one byte, left the capacity at 8, and let the
/// 9 pushes reallocate.
#[test]
fn reserve_batch_capacity_covers_reused_buffer() {
    for batch in [9usize, 12, 16, 17, 64] {
        let mut out: Vec<Bytes> = Vec::with_capacity(8);
        out.push(Bytes::from_static(b"previous cycle"));
        reserve_batch_capacity(&mut out, batch);
        assert!(
            out.capacity() >= batch,
            "batch={batch} cap={} — reserve must count from len",
            out.capacity()
        );
        assert_eq!(out.len(), 0, "reuse buffer must be cleared");
    }
}

/// Output order and packet count are unchanged by the reservation fix.
#[test]
fn batch_ref_into_preserves_order_and_count() {
    let (crypt, _) = CryptEngine::select("aes-128", b"test-key-12345678");
    let cb = parking_lot::Mutex::new(CryptoBuf::new(0xDEADBEEF));
    for batch in [9usize, 12, 16] {
        let packets: Vec<Bytes> = (0..batch).map(|i| Bytes::from(vec![i as u8; 32])).collect();
        let mut out: Vec<Bytes> = Vec::with_capacity(8);
        encrypt_batch_ref_into(&packets, &crypt, &cb, true, false, &mut out);
        assert_eq!(out.len(), batch, "one output packet per input");
        for (i, enc) in out.iter_mut().enumerate() {
            let mut buf = enc.to_vec();
            let dec = cb
                .lock()
                .decrypt_cfb(&mut buf, &crypt)
                .expect("decrypt")
                .to_vec();
            assert_eq!(dec, vec![i as u8; 32], "packet {i} of {batch}");
        }
    }
}
