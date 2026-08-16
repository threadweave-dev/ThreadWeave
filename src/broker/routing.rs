/// Returns the broker destination dedicated to a Worker identity.
///
/// Worker names are deliberately not part of transport routing: they are
/// human-readable metadata and are not required to be globally unique.
pub fn worker_destination(worker_id: &str) -> String {
    format!("workers.{worker_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_destination_is_deterministic_and_uses_the_worker_id() {
        assert_eq!(worker_destination("worker-1"), "workers.worker-1");
        assert_eq!(worker_destination("worker-1"), "workers.worker-1");
        assert_ne!(
            worker_destination("worker-1"),
            worker_destination("worker-2")
        );
    }
}
