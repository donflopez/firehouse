use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, TlsMode};
use serde_json::Value;

pub struct PG {
    fields: Vec<&'static str>,
    url: &'static str,
    database: &'static str,
    user: &'static str,
    password: &'static str,
    pool: Pool<PostgresConnectionManager>,
}

impl PG {
    pub fn new(
        fields: Vec<&'static str>,
        url: &'static str,
        database: &'static str,
        password: &'static str,
        user: &'static str,
    ) -> Self {
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
        }
    }
    /// Adds one to the number given.
    ///
    /// # Examples
    ///
    /// ```
    /// let arg = 5;
    /// let answer = firehouse::insert(arg);
    ///
    /// assert_eq!(6, answer);
    /// ```
    pub fn insert(&self, data: Value) {
        if data.is_object() {
            let obj = data.as_object().expect("Object extraction failed");
            let mut vals: String = s!("");

            for k in self.fields.clone() {
                let val = obj
                    .get(k)
                    .expect("Cannot get value from map")
                    .as_str()
                    .expect("Cannot cast to str");
                vals = format!("{}{}", vals, s!(val));
            }
        }
    }
}
