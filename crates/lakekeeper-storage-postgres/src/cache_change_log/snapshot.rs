use std::{fmt, str::FromStr};

/// A Postgres `pg_snapshot`: transactions below `xmin` are finished, those at or above
/// `xmax` had not started, and `xip` lists the ones still running in between.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PgSnapshot {
    pub(super) xmin: u64,
    pub(super) xmax: u64,
    pub(super) xip: Vec<u64>,
}

impl PgSnapshot {
    /// The smallest xid whose rows a poll from this snapshot can still return: the oldest
    /// running transaction, or `xmax` when none was running.
    pub(super) fn first_unseen(&self) -> u64 {
        self.xip.iter().copied().min().unwrap_or(self.xmax)
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("Invalid pg_snapshot {0:?}")]
pub(super) struct InvalidSnapshot(String);

impl FromStr for PgSnapshot {
    type Err = InvalidSnapshot;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidSnapshot(s.to_string());
        let mut parts = s.split(':');
        let (Some(xmin), Some(xmax), Some(xip), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(invalid());
        };
        let xmin = xmin.parse().map_err(|_| invalid())?;
        let xmax = xmax.parse().map_err(|_| invalid())?;
        let xip = if xip.is_empty() {
            Vec::new()
        } else {
            xip.split(',')
                .map(str::parse)
                .collect::<Result<_, _>>()
                .map_err(|_| invalid())?
        };
        Ok(Self { xmin, xmax, xip })
    }
}

impl fmt::Display for PgSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:", self.xmin, self.xmax)?;
        for (i, xid) in self.xip.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{xid}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_prints_postgres_text() {
        for text in ["10:10:", "10:15:10,12", "742:745:742"] {
            assert_eq!(text.parse::<PgSnapshot>().unwrap().to_string(), text);
        }
        assert_eq!(
            "10:15:10,12".parse::<PgSnapshot>().unwrap(),
            PgSnapshot {
                xmin: 10,
                xmax: 15,
                xip: vec![10, 12]
            }
        );
    }

    #[test]
    fn rejects_other_text() {
        for text in [
            "",
            "10",
            "10:15",
            "10:15:1:2",
            "a:15:",
            "10:15:x",
            "10:15:1,",
        ] {
            assert_eq!(
                text.parse::<PgSnapshot>(),
                Err(InvalidSnapshot(text.to_string()))
            );
        }
    }

    #[test]
    fn first_unseen_is_the_oldest_running_transaction_or_xmax() {
        assert_eq!(
            "10:15:12,11".parse::<PgSnapshot>().unwrap().first_unseen(),
            11
        );
        assert_eq!("10:15:".parse::<PgSnapshot>().unwrap().first_unseen(), 15);
    }
}
