

pub struct PostgresMetastore {
    pg_client: postgres::Client,

    // Keep a copy of all indexes, 
    // mem_db: redb::Database,
}
