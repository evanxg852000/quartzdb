use sqlx::{migrate::MigrateDatabase, FromRow, Row, Sqlite, SqlitePool};



#[derive(Clone, FromRow, Debug)]
struct IndexMeta {
    name: String,
}
