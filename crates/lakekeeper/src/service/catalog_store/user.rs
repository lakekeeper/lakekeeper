use crate::api::management::v1::user::User;

#[derive(Debug, Clone)]
pub enum CreateOrUpdateUserResponse {
    Created(User),
    Updated(User),
}
