use std::time::Duration;

pub fn should_checkpoint_autocommit(
    force_checkpoint: bool,
    commits_since_checkpoint: u64,
    checkpoint_every_commits: u64,
    elapsed_since_checkpoint: Duration,
    checkpoint_interval: Duration,
) -> bool {
    if force_checkpoint {
        return true;
    }
    let interval_hit =
        !checkpoint_interval.is_zero() && elapsed_since_checkpoint >= checkpoint_interval;
    let commit_count_hit =
        checkpoint_every_commits > 0 && commits_since_checkpoint >= checkpoint_every_commits;
    interval_hit || commit_count_hit
}

#[cfg(test)]
mod tests {
    use super::should_checkpoint_autocommit;
    use std::time::Duration;

    #[test]
    fn force_checkpoint_always_wins() {
        assert!(should_checkpoint_autocommit(
            true,
            0,
            0,
            Duration::from_secs(0),
            Duration::from_secs(0)
        ));
    }

    #[test]
    fn commit_threshold_triggers_checkpoint() {
        assert!(should_checkpoint_autocommit(
            false,
            5,
            5,
            Duration::from_millis(1),
            Duration::from_secs(60)
        ));
        assert!(!should_checkpoint_autocommit(
            false,
            4,
            5,
            Duration::from_millis(1),
            Duration::from_secs(60)
        ));
    }

    #[test]
    fn interval_threshold_triggers_checkpoint() {
        assert!(should_checkpoint_autocommit(
            false,
            1,
            100,
            Duration::from_secs(10),
            Duration::from_secs(5)
        ));
        assert!(!should_checkpoint_autocommit(
            false,
            1,
            100,
            Duration::from_secs(4),
            Duration::from_secs(5)
        ));
    }
}
