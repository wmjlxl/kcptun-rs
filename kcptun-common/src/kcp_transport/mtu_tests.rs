use super::validate_encrypted_mtu;

#[test]
fn encrypted_mtu_boundaries_match_receive_slot() {
    for (method, maximum) in [
        ("null", 2048),
        ("none", 2028),
        ("aes", 2028),
        ("aes-128-gcm", 2020),
    ] {
        assert!(validate_encrypted_mtu(maximum, method).is_ok(), "{method}");
        assert!(
            validate_encrypted_mtu(maximum + 1, method).is_err(),
            "{method}"
        );
    }
}
