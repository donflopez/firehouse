use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use serde_json::{Value};
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use sled::{ConfigBuilder, Db};
use std::thread;
use tokio::timer::Delay;
use tokio::prelude::*;

#[derive(Deserialize)]
struct TenantReq {
	tenant: String,
	id: String,
}

impl TenantReq {
	pub fn key(&self) -> String {
		format!("{}/{}", self.tenant, self.id)
	}
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Stats {
	pub totalSent: usize,
	pub totalReceived: usize,
	pub totalRepeated: usize,
	pub totalDelayed: usize,
	pub totalDelayedResponse: usize,
	pub totalErrored: usize,
	timestamp: String,
}

#[derive(Deserialize, Serialize)]
pub struct TenantCheck {
	name: String,
	id: String,
	sent: bool,
	received: bool,
	tries: usize,
	errors_sent: usize,
	time_sent: String,
	time_received: Option<String>,
}

impl TenantCheck {
	pub fn new(name: String, id: String) -> Self {
		TenantCheck {
			name,
			id,
			sent: true,
			received: false,
			tries: 0,
			errors_sent: 0,
			time_sent: SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.unwrap()
				.as_millis()
				.to_string(),
			time_received: None,
		}
	}
}

fn index(tenant: web::Bytes, db: web::Data<sled::Db>) -> impl Responder {
	let logs_string = String::from_utf8(tenant.to_vec()).expect("Can't parse string");
	let logs: Vec<_> = logs_string.split('\n').collect();
	// let mut stats: Stats = serde_json::from_slice(
	// 		&db.get("stats")
	// 			.expect("Cannot get item from slad")
	// 			.unwrap()
	// 			.to_vec()[..],
	// 	)
	// 	.expect("Cannot parse check from sled");
	//  ::thread::sleep(Duration::from_millis(rand::thread_rng().gen_range(1, 500)));

	// stats.totalDelayedResponse += logs.len();
	
	// db.merge("stats", serde_json::to_vec(&stats).unwrap())
	// 	.expect("cannot update stats from server");
	// let key = tenant.key();
	// println!(
	// 	"{:?}",
	// 	String::from_utf8(
	// 		db.get(key.clone())
	// 			.expect("Cannot get item")
	// 			.unwrap()
	// 			.to_vec()
	// 	)
	// );
	// println!("OK! Stats: {:?}", stats);
	if rand::thread_rng().gen_range(1, 10) == 7 {
		HttpResponse::ServiceUnavailable().body("Not ok")
	}
	else {
		for l in logs.iter() {
			// println!("Iterating on logs {}", l.clone());
			// println!("req for {}", l.clone());
			let l: Value = serde_json::from_str(&l).expect("Cannot parse log json");
			// let key = l.key();
			// db.set(key, vec![1]).unwrap();
			// let mut check: TenantCheck = serde_json::from_slice(
			// 	&db.get(key.clone())
			// 		.expect("Cannot get item from slad")
			// 		.unwrap()
			// 		.to_vec()[..],
			// )
			// .expect("Cannot parse check from sled");

			// if (check.received) {
			// 	stats.totalRepeated += 1;
			// } else {
			// 	check.time_received = Some(
			// 		SystemTime::now()
			// 			.duration_since(UNIX_EPOCH)
			// 			.unwrap()
			// 			.as_millis()
			// 			.to_string(),
			// 	);
			// }

			// check.received = true;
			// check.tries += 1;

			db.merge("stats_total", vec![1]).unwrap();
			// stats.totalReceived += 1;

			// db.set("stats", serde_json::to_vec(&stats).unwrap())
			// 	.expect("cannot update stats from server");
			// db.set(key, serde_json::to_vec(&check).unwrap())
			// 	.expect("cannot update check from server");
		}

		HttpResponse::Ok().body("OK!")
	}
}

fn index2() -> impl Responder {
	HttpResponse::Ok().body("Hello world again!")
}

pub fn create_web_server(tree: sled::Db) {
	HttpServer::new(move || {
		App::new()
			.data(tree.clone())
			.route("/check/{tenant}", web::post().to(index))
	})
	.bind("127.0.0.1:7777")
	.unwrap()
	.run()
	.unwrap();
}

fn concatenate_merge(
	_key: &[u8],               // the key being merged
	old_value: Option<&[u8]>,  // the previous value, if one existed
	merged_bytes: &[u8]        // the new bytes being merged in
	) -> Option<Vec<u8>> {       // set the new value, return None to delete
		// let mut old: Stats = serde_json::from_slice(old_value.unwrap()).unwrap();
		// let new: Stats = serde_json::from_slice(merged_bytes).unwrap();
		
		// old.totalReceived += new.totalReceived;
		// old.totalRepeated += new.totalRepeated;
		let str_u32 = String::from_utf8(old_value.expect("No old value present").to_vec()).expect("Cannot parse utf8").replace("\"", "");
		let old: u32 = str_u32.as_str().parse().expect("Cannot parse to usize");
		let new = old + 1;

		Some(Vec::from(new.to_string()))
}

fn main () {
	let config = ConfigBuilder::new()
        .temporary(true)
        .merge_operator(concatenate_merge)
        .build();

	let tree = Db::start(config).expect("Cannot open db");
	tree.set("stats_total", serde_json::to_vec(&(0 as u32).to_string()).expect("Cannot serialize stats")).expect("Set didn't work");
	let tree2 = tree.clone();

	thread::spawn(move || {
		loop {
			let total: Value = serde_json::from_slice(&tree2.get("stats_total").unwrap().unwrap().to_vec()[..]).unwrap();
			println!("Total received: {}", total);
			thread::sleep(Duration::from_millis(1000));
		}
	});

	create_web_server(tree);
}