use assert_cmd::cargo_bin_cmd;
use fluree_db_credential::{verify_jws, EventsTokenPayload};

#[test]
fn request_selection_credential_is_explicit_signed_and_scoped() {
    // Fixed fixture key, used only to verify the CLI's signed output locally.
    let key = format!("0x{}", "42".repeat(32));
    for select_policy in [false, true] {
        let mut cmd = cargo_bin_cmd!("fluree");
        cmd.args([
            "token",
            "create",
            "--private-key",
            &key,
            "--audience",
            "test-db",
            "--read-ledger",
            "books:main",
        ]);
        if select_policy {
            cmd.arg("--policy-select");
        }
        let output = cmd.assert().success().get_output().stdout.clone();
        let token = String::from_utf8(output).unwrap();
        let verified = verify_jws(token.trim()).unwrap();
        let claims: EventsTokenPayload = serde_json::from_str(&verified.payload).unwrap();
        claims
            .validate(Some("test-db"), &verified.did, false)
            .unwrap();
        assert_eq!(
            matches!(
                claims.fluree_policy,
                Some(fluree_db_credential::jwt_claims::PolicyClaim::Request(_))
            ),
            select_policy
        );
        assert_eq!(
            claims.ledger_read_ledgers,
            Some(vec!["books:main".to_string()])
        );
        assert!(!claims.has_ledger_write_permissions());
    }
}

#[test]
fn request_selection_creation_requires_an_audience() {
    cargo_bin_cmd!("fluree")
        .args([
            "token",
            "create",
            "--private-key",
            "0x4242424242424242424242424242424242424242424242424242424242424242",
            "--read-ledger",
            "books:main",
            "--policy-select",
        ])
        .assert()
        .failure()
        .stderr(predicates::str::contains("--audience"));
}
