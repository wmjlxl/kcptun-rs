use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn request(port: u16, bytes: &[u8]) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream.write_all(bytes).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn slow_client_does_not_block_other_requests_and_large_body_is_rejected() {
    knet::block_on(async {
        let probe = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = stop.clone();
        let addr = format!("127.0.0.1:{port}");
        let server = knet::spawn_task(async move {
            kpprof::run_pprof(&addr, server_stop).await.unwrap();
        });
        let slow = loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => break stream,
                Err(_) => knet::sleep_ms(10).await,
            }
        };
        let response = std::thread::spawn(move || {
            let index = request(port, b"GET /debug/pprof/ HTTP/1.1\r\n\r\n");
            let too_large = request(
                port,
                b"POST /debug/pprof/symbol HTTP/1.1\r\nContent-Length: 65537\r\n\r\n",
            );
            (index, too_large)
        })
        .join()
        .unwrap();
        assert!(response.0.starts_with("HTTP/1.1 200"), "{}", response.0);
        assert!(response.1.starts_with("HTTP/1.1 413"), "{}", response.1);
        drop(slow);
        stop.store(true, Ordering::Release);
        knet::timeout(Duration::from_secs(2), server)
            .await
            .expect("pprof accept loop should stop")
            .unwrap();
    });
}
