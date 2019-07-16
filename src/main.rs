#[macro_use]
extern crate serde_derive;
extern crate amiquip;
extern crate bytes;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;
extern crate redis;
extern crate serde_yaml;

use amiquip::{Connection, Exchange, Publish};
use rand::prelude::*;
use redis::Commands;
use serde_json::{from_str, Value};
use std::str;
use std::time::Duration;

#[macro_use]
mod utils;
mod firehouse;
mod kinesis;
mod postgresql;

fn get_tenant(l: &Value) -> String {
	l["metadata"]["tenant"].to_string()
}

fn get_type(l: &Value) -> String {
	l["metadata"]["type"].to_string()
}

fn get_sink(conf: &Value) -> String {
	conf["sink"].to_string()
}

fn get_topic(conf: &Value, l: &Value) -> String {
	let tenant = get_tenant(l);
	let log_type = get_type(l);
	let destination = get_sink(conf);

	format!("all.{}.{}.{}", destination, log_type, tenant)
}

#[derive(Deserialize, Serialize)]
struct Sink {
	url: String,
	batch: usize,
	interval: Duration,
}

#[derive(Deserialize, Serialize)]
struct Config {
	tenant: String,
	sinks: Vec<Sink>,
}

impl Config {
	pub fn new(tenant: String) -> Self {
		let n_sinks: usize = rand::thread_rng().gen_range(1, 3);

		let sinks: Vec<Sink> = [0..n_sinks]
			.into_iter()
			.map(|_| Sink {
				url: format!("http://192.169.1.129/check/{}", tenant),
				batch: rand::thread_rng().gen_range(1, 1000),
				interval: Duration::new(rand::thread_rng().gen_range(1, 300), 0),
			})
			.collect::<Vec<Sink>>();

		Config { tenant, sinks }
	}
}

fn main() {
	let client =
		redis::Client::open("redis://127.0.0.1:32768/").expect("Cannot open redis connection");
	let mut con = client
		.get_connection()
		.expect("Cannot get redis connection");

	let k_handler =
		kinesis::KinesisHandler::new(s!("test"), None, Some("https://192.168.1.129:4568"));
	let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")
		.expect("Cannot connect to RabbitMQ");

	let channel = connection.open_channel(None).expect("Cannot open channel");
	let exchange = Exchange::direct(&channel);

	let receiver = k_handler.get_records_stream(None, None);
	println!("Starting to pool kinesis");
	loop {
		let r = receiver.recv().unwrap();
		let l: Value =
			from_str(str::from_utf8(r.data.as_ref()).unwrap()).expect("JSON cannot be parsed");

		let tenant = get_tenant(&l);
		println!("Record for tenant: {}", tenant.clone());
		// Log on its shape
		match con.get(tenant.clone()) {
			Ok(c) => {
				let conf_js: String = c;
				let conf: Value = from_str(&conf_js).expect("Cannot parse configuration");
				let topic = get_topic(&conf, &l);
				let l_string = l.to_string();
				let message = Publish::new(l_string.as_bytes(), topic.clone());
				println!("Publishing topic: {}", topic);
				exchange
					.publish(message)
					.expect("Error while publishing a message");
			}
			Err(e) => {
				let conf_str = serde_json::to_string(&Config::new(tenant.clone())).unwrap();
				let _: () = con
					.set(tenant.clone(), conf_str.clone())
					.expect("Write to work");

				let t_list: String = con.get("tenant_list").unwrap();
				let _: () = con
					.set("tenant_list", format!("{},{}", t_list, tenant.clone()))
					.unwrap();

				let _: () = con
					.publish("tenant_config", conf_str)
					.expect("Write to work");

				println!(
					"No config for tenant... {}, just created one, this log is dropped",
					tenant
				);
			}
		}
	}
}
