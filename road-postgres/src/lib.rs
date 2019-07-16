use postgres::{rows::Rows, types::ToSql};
use std::thread;

use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, TlsMode};
use rayon::{ThreadPool, ThreadPoolBuilder};
use serde_json::{json, Value};

fn build_insert_query(
    table: &str,
    fields: Vec<&str>,
    data: Value,
) -> Option<(String, Vec<String>)> {
    if data.is_object() && fields.len() > 0 {
        let obj = data.as_object().expect("Object extraction failed");
        let mut vals: Vec<String> = Vec::new();
        let mut fields_concat = "".to_owned();

        for k in fields.clone() {
            let val = obj.get(k).expect("Cannot get value from map");

            fields_concat = format!("{}, {}", fields_concat, k.to_owned());
            vals.push(val.as_str().expect("Cannot parste to string").to_owned());
        }

        return Some((
            format!(
                "INSERT INTO {} ({}) VALUES ({});",
                table,
                fields_concat
                    .get(2..fields_concat.len())
                    .expect("Cannot get fields string"),
                (0..fields.len())
                    .map(|v| format!("${}", (v + 1).to_string()))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            vals,
        ));
    }

    None
}

pub struct PG {
    fields: Vec<&'static str>,
    url: &'static str,
    database: &'static str,
    user: &'static str,
    password: &'static str,
    pool: Pool<PostgresConnectionManager>,
    th_pool: ThreadPool,
}

impl PG {
    pub fn new(
        fields: Vec<&'static str>,
        url: &'static str,
        database: &'static str,
        password: &'static str,
        user: &'static str,
    ) -> Self {
        let th = ThreadPoolBuilder::new()
            .num_threads(100)
            .build()
            .expect("Thread pool not created");

        PG {
            fields,
            url,
            database,
            password,
            user,
            pool: Pool::new(
                PostgresConnectionManager::new(
                    format!("postgres://{}:{}@{}/{}", user, password, url, database),
                    TlsMode::None,
                )
                .expect("Postgres connection manager failed to create"),
            )
            .expect("Postgres connection pool failed"),
            th_pool: th,
        }
    }

    pub fn insert(&self, data: Value, table: &str) {
        let (insert_statement, vals) =
            build_insert_query(table, self.fields.clone(), data).expect("Cannot build statement");

        let pool = self.pool.clone();

        self.th_pool.spawn(move || {
            let conn = pool.get().expect("Cannot get a connection");
            let params: Vec<&ToSql> = vals
                .iter()
                .map(|v| {
                    let v: &ToSql = v;
                    v
                })
                .collect();
            println!("{:?}", params);
            println!("{:?}", insert_statement);
            conn.execute(&insert_statement, params.as_slice())
                .expect("INSERT statement failed");
        });
    }

    pub fn custom<F, F2>(&self, statement: &'static str, on_success: F, on_failure: F2)
    where
        F: FnOnce(String) + Send + Sync + 'static,
        F2: FnOnce(String) + Send + Sync + 'static,
    {
        let pool = self.pool.clone();

        self.th_pool.spawn(move || {
            let conn = pool.get().expect("Cannot get a connection");

            match conn.execute(&statement, &[]) {
                Ok(v) => {
                    on_success(v.to_string());
                }
                Err(e) => {
                    on_failure(format!("{}", e));
                }
            }
        })
    }

    pub fn query<F, F2>(&self, query: &'static str, on_success: F, on_failure: F2)
    where
        F: FnOnce(Rows) + Send + Sync + 'static,
        F2: FnOnce(String) + Send + Sync + 'static,
    {
        let pool = self.pool.clone();

        self.th_pool.spawn(move || {
            let conn = pool.get().expect("Cannot get a connection");

            match conn.query(&query, &[]) {
                Ok(v) => {
                    on_success(v);
                }
                Err(e) => {
                    on_failure(format!("{}", e));
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn build_insert_statement() {
        use crate::build_insert_query;
        use serde_json::{json, Value};

        let fields = vec!["__id", "type", "date", "description"];
        let (statement, vals) = build_insert_query(
            "my_table",
            fields,
            json!({"__id": "i2382832", "type": "s", "date":"2018-29-12", "description":"Not sure what the desc is."}),
        )
        .expect("Cannot get a value");

        assert_eq!(
            "INSERT INTO my_table (__id, type, date, description) VALUES ($1, $2, $3, $4);",
            statement
        );

        assert_eq!(
            vec!["i2382832", "s", "2018-29-12", "Not sure what the desc is."],
            vals
        );
    }
}
