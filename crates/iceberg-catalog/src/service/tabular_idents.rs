use std::{
    fmt::{Display, Formatter},
    ops::Deref,
};

use iceberg::TableIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use super::{TableId, ViewId};

#[derive(Hash, PartialOrd, PartialEq, Debug, Clone, Copy, Eq, Deserialize, ToSchema)]
#[serde(tag = "type", content = "id", rename_all = "kebab-case")]
#[schema(as=TabularIdentUuid)]
pub enum TabularId {
    Table(Uuid),
    View(Uuid),
}

impl TabularId {
    #[must_use]
    pub fn typ_str(&self) -> &'static str {
        match self {
            TabularId::Table(_) => "Table",
            TabularId::View(_) => "View",
        }
    }
}

impl From<TableId> for TabularId {
    fn from(ident: TableId) -> Self {
        TabularId::Table(ident.0)
    }
}

impl From<ViewId> for TabularId {
    fn from(ident: ViewId) -> Self {
        TabularId::View(ident.0)
    }
}

impl AsRef<Uuid> for TabularId {
    fn as_ref(&self) -> &Uuid {
        match self {
            TabularId::Table(id) | TabularId::View(id) => id,
        }
    }
}

impl Display for TabularId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &**self)
    }
}

// We get these two types since we are using them as HashMap keys. Those need to be sized,
// implementing these types via Cow makes them not sized, so we go for two... not ideal.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum TabularIdentBorrowed<'a> {
    Table(&'a TableIdent),
    #[allow(dead_code)]
    View(&'a TableIdent),
}

impl TabularIdentBorrowed<'_> {
    pub(crate) fn typ_str(&self) -> &'static str {
        match self {
            TabularIdentBorrowed::Table(_) => "Table",
            TabularIdentBorrowed::View(_) => "View",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TabularIdentOwned {
    Table(TableIdent),
    View(TableIdent),
}

impl TabularIdentOwned {
    pub(crate) fn into_inner(self) -> TableIdent {
        match self {
            TabularIdentOwned::Table(ident) | TabularIdentOwned::View(ident) => ident,
        }
    }

    pub(crate) fn into_table(self) -> crate::api::Result<TableIdent> {
        match self {
            TabularIdentOwned::Table(ident) => Ok(ident),
            TabularIdentOwned::View(_) => Err(ErrorModel::internal(
                "Expected a table identifier, but got a view identifier",
                "UnexpectedViewIdentifier",
                None,
            )
            .into()),
        }
    }

    pub(crate) fn into_view(self) -> crate::api::Result<TableIdent> {
        match self {
            TabularIdentOwned::Table(_) => Err(ErrorModel::internal(
                "Expected a view identifier, but got a table identifier",
                "UnexpectedTableIdentifier",
                None,
            )
            .into()),
            TabularIdentOwned::View(ident) => Ok(ident),
        }
    }
}

impl<'a> From<TabularIdentBorrowed<'a>> for TabularIdentOwned {
    fn from(ident: TabularIdentBorrowed<'a>) -> Self {
        match ident {
            TabularIdentBorrowed::Table(ident) => TabularIdentOwned::Table(ident.clone()),
            TabularIdentBorrowed::View(ident) => TabularIdentOwned::View(ident.clone()),
        }
    }
}

impl TabularIdentBorrowed<'_> {
    pub(crate) fn to_table_ident_tuple(&self) -> &TableIdent {
        match self {
            TabularIdentBorrowed::Table(ident) | TabularIdentBorrowed::View(ident) => ident,
        }
    }
}

impl Deref for TabularId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        match self {
            TabularId::Table(id) | TabularId::View(id) => id,
        }
    }
}
