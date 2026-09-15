//! Regressions for the isolated object_store / reqwest compatibility patch.
use delta_kernel::object_store::{
    http::{HttpBuilder, HttpStore},
    path::Path,
    Certificate, ClientOptions, ObjectStoreExt, RetryConfig,
};
use flate2::{write::GzEncoder, Compression};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use std::{io::Write, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};
use tokio_rustls::{rustls, TlsAcceptor};

struct Server {
    url: String,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn respond(mut stream: impl AsyncRead + AsyncWrite + Unpin, body: &[u8], gzip: bool) {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let byte = stream.read_u8().await.unwrap();
        request.push(byte);
        assert!(request.len() < 16_384);
    }
    let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
    let (status, range, content) = if request.contains("\r\nrange: bytes=2-5\r\n") {
        (
            "206 Partial Content",
            format!("Content-Range: bytes 2-5/{}\r\n", body.len()),
            &body[2..6],
        )
    } else {
        assert!(
            !request.contains("\r\nrange:"),
            "unexpected byte range: {request}"
        );
        ("200 OK", String::new(), body)
    };
    let encoding = if gzip {
        "Content-Encoding: gzip\r\n"
    } else {
        ""
    };
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{range}{encoding}Last-Modified: Mon, 14 Sep 2026 12:00:00 GMT\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n",
        content.len()
    );
    stream.write_all(headers.as_bytes()).await.unwrap();
    if !request.starts_with("head ") {
        stream.write_all(content).await.unwrap();
    }
    stream.shutdown().await.unwrap();
}

async fn server(body: Vec<u8>, gzip: bool, tls: Option<rustls::ServerConfig>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scheme = if tls.is_some() { "https" } else { "http" };
    let url = format!(
        "{scheme}://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let acceptor = tls.map(|config| TlsAcceptor::from(Arc::new(config)));
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            if let Some(acceptor) = &acceptor {
                // The rejection checks deliberately abort the TLS handshake.
                if let Ok(stream) = acceptor.accept(socket).await {
                    respond(stream, &body, gzip).await;
                }
            } else {
                respond(socket, &body, gzip).await;
            }
        }
    });
    Server { url, task }
}

fn store(server: &Server, options: ClientOptions) -> HttpStore {
    HttpBuilder::new()
        .with_url(&server.url)
        .with_client_options(options.with_timeout(Duration::from_secs(5)))
        .with_retry(RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .build()
        .unwrap()
}

#[tokio::test]
async fn encoded_objects_and_ranges_preserve_bytes() {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(b"object bytes must not be transparently decompressed")
        .unwrap();
    let compressed = encoder.finish().unwrap();
    let server = server(compressed.clone(), true, None).await;
    let store = store(&server, ClientOptions::new().with_allow_http(true));
    let path = Path::from("data");
    assert_eq!(
        store.head(&path).await.unwrap().size,
        compressed.len() as u64
    );
    assert_eq!(
        store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        compressed
    );
    assert_eq!(
        store.get_range(&path, 2..6).await.unwrap().as_ref(),
        &compressed[2..6]
    );
    assert!(!server.task.is_finished(), "test server failed");
}

#[tokio::test]
async fn custom_ca_is_required_and_does_not_disable_hostname_verification() {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca_params, ca_key);
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["localhost".to_owned()])
        .unwrap()
        .signed_by(&leaf_key, &issuer)
        .unwrap();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(leaf_key.serialize_der()).into(),
        )
        .unwrap();
    let mut server = server(b"TLS object".to_vec(), false, Some(config)).await;
    let path = Path::from("data");
    let error = store(&server, ClientOptions::new())
        .get(&path)
        .await
        .unwrap_err();
    assert!(
        format!("{error:?}")
            .to_ascii_lowercase()
            .contains("certificate"),
        "unexpected failure: {error:?}"
    );
    let options =
        ClientOptions::new().with_root_certificate(Certificate::from_der(ca.der()).unwrap());
    assert_eq!(
        store(&server, options.clone())
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"TLS object"
    );
    // The same CA must not authorize a hostname absent from the leaf's SANs.
    server.url = server.url.replace("localhost", "127.0.0.1");
    let error = store(&server, options).get(&path).await.unwrap_err();
    assert!(
        format!("{error:?}")
            .to_ascii_lowercase()
            .contains("certificate"),
        "unexpected failure: {error:?}"
    );
    assert!(!server.task.is_finished(), "test server failed");
}
