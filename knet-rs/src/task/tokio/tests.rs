use super::*;
use std::time::Duration;

#[test]
fn panicking_cpu_jobs_do_not_strand_callers_or_kill_workers() {
    block_on(async {
        for _ in 0..12 {
            let task = tokio::spawn(async { cpu_block(|| -> usize { panic!("injected") }).await });
            let result = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .expect("panicking cpu job must finish on the caller")
                .expect_err("the original panic must reach the caller");
            assert!(result.is_panic());
        }
        assert_eq!(cpu_block(|| 42).await, 42);
    });
}

#[test]
fn cancelled_cpu_job_does_not_deliver_to_a_reused_result_pair() {
    block_on(async {
        let timed_out = tokio::time::timeout(
            Duration::from_millis(10),
            cpu_block(|| {
                std::thread::sleep(Duration::from_millis(80));
                1usize
            }),
        )
        .await;
        assert!(timed_out.is_err());
        assert_eq!(cpu_block(|| 2usize).await, 2);
    });
}
