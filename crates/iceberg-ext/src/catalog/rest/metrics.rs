use crate::models;
use validator::Validate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "report-type", rename_all = "kebab-case")]
pub enum ReportMetricsRequest {
    ScanReport(models::ScanReport),
    CommitReport(models::CommitReport),
}

impl Validate for ReportMetricsRequest {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            ReportMetricsRequest::ScanReport(x) => x.validate(),
            ReportMetricsRequest::CommitReport(x) => x.validate(),
        }
    }
}
