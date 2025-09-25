#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ViewRequirement {
    AssertViewUuid(AssertViewUuid),
}

/// The view UUID must match the requirement's `uuid`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AssertViewUuid {
    pub uuid: uuid::Uuid,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_assert_view_uuid() {
        let j = serde_json::json!({
            "type": "assert-view-uuid",
            "uuid": "550e8400-e29b-41d4-a716-446655440001"
        });

        let r: ViewRequirement = serde_json::from_value(j.clone()).unwrap();
        match r.clone() {
            ViewRequirement::AssertViewUuid(x) => {
                assert_eq!(
                    x.uuid,
                    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap()
                );
            }
        }
        assert_eq!(serde_json::to_value(&r).unwrap(), j);
    }
}
