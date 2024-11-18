use super::{TableIdentUuid, ViewIdentUuid};
use iceberg::TableIdent;
use iceberg_ext::catalog::rest::ErrorModel;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use uuid::Uuid;

#[derive(Hash, PartialOrd, PartialEq, Debug, Clone, Copy, Eq)]
pub enum TabularIdentUuid {
    Table(Uuid),
    View(Uuid),
}

impl TabularIdentUuid {
    #[must_use]
    pub fn typ_str(&self) -> &'static str {
        match self {
            TabularIdentUuid::Table(_) => "Table",
            TabularIdentUuid::View(_) => "View",
        }
    }
}

impl From<TableIdentUuid> for TabularIdentUuid {
    fn from(ident: TableIdentUuid) -> Self {
        TabularIdentUuid::Table(ident.0)
    }
}

impl From<ViewIdentUuid> for TabularIdentUuid {
    fn from(ident: ViewIdentUuid) -> Self {
        TabularIdentUuid::View(ident.0)
    }
}

impl AsRef<Uuid> for TabularIdentUuid {
    fn as_ref(&self) -> &Uuid {
        match self {
            TabularIdentUuid::Table(id) | TabularIdentUuid::View(id) => id,
        }
    }
}

impl Display for TabularIdentUuid {
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

impl<'a> TabularIdentBorrowed<'a> {
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

    #[allow(dead_code)]
    pub(crate) fn as_table(&self) -> crate::api::Result<&TableIdent> {
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

    #[allow(dead_code)]
    pub(crate) fn as_view(&self) -> crate::api::Result<&TableIdent> {
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

impl<'a> TabularIdentBorrowed<'a> {
    pub(crate) fn to_table_ident_tuple(&self) -> &TableIdent {
        match self {
            TabularIdentBorrowed::Table(ident) | TabularIdentBorrowed::View(ident) => ident,
        }
    }
}

impl Deref for TabularIdentUuid {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        match self {
            TabularIdentUuid::Table(id) | TabularIdentUuid::View(id) => id,
        }
    }
}
