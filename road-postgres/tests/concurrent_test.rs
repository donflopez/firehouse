// extern crate we're testing, same as any other code will do.
extern crate dockers;
extern crate road_postgres;

// importing common module.
mod setup;
use postgres::rows::Rows;
use road_postgres::PG;
use serde_json::json;
use setup::{run_postgres, stop_postgres};
use std::{thread, time::Duration};

#[test]
fn test_add() {
    run_postgres();
    // stop_postgres();
    // TODO: change this for a health check on PG
    thread::sleep(Duration::from_millis(10000));

    let fields = vec!["__id", "type", "date", "description"];

    let pg = PG::new(fields, "localhost:5432", "postgres", "postgres", "postgres");

    pg.custom(
        "
        CREATE TABLE road_postgres (
            __id    text NOT NULL,
            type    text NOT NULL,
            date    timestamp,
            description text
        );
    ",
        |res| println!("Response: {}", res),
        |e| println!("Table already exists: {}", e),
    );

    thread::sleep(Duration::from_millis(1000));

    pg.insert(
        json!({
            "__id":"123456",
            "type": "a_type",
            "date": "2019-03-12 10:20:12",
            "description": "Example log",
        }),
        "road_postgres",
    );

    thread::sleep(Duration::from_millis(1000));

    pg.query(
        "
        SELECT count(*) FROM road_postgres;
    ",
        |res: Rows| {
            println!("RESPONSE:::: {}", res.len());
            let first_row = res.get(0);
            println!("{:?}", first_row);
            let count: i64 = first_row.get(0);
            assert_eq!(1, count);
        },
        |_| {},
    );

    stop_postgres();

    // TODO: wait for the threads to finish
    thread::sleep(Duration::from_millis(1000));
}
