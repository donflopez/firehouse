#[macro_use]
extern crate serde_derive;
extern crate amiquip;
extern crate bytes;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;
extern crate redis;
extern crate serde_yaml;

use amiquip::{Connection, Exchange, Publish, ExchangeType, ExchangeDeclareOptions};
use rand::prelude::*;
use redis::Commands;
use serde_json::{from_str, Value};
use std::{str, thread};
use std::time::Duration;
// Scheduler, and trait for .seconds(), .minutes(), etc.
use clokwerk::{Scheduler, TimeUnits};
// Import week days and WeekDay
use clokwerk::Interval::*;
use crossbeam::channel::unbounded;

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
	conf["sink"]["name"].to_string()
}

fn get_topic(conf: &Value, l: &Value) -> String {
	let tenant = get_tenant(l);
	let log_type = get_type(l);
	let destination = get_sink(conf);

	format!("all.{}.{}.{}", destination, log_type, tenant).replace("\"", "")
}

fn get_count_key(conf: &Value, l: &Value) -> String {
	let tenant = get_tenant(l);
	let destination = get_sink(conf);

	format!("count_{}_{}", tenant, destination).replace("\"", "")
}

#[derive(Deserialize, Serialize, Clone)]
struct Sink {
	name: String,
	url: String,
	batch: usize,
	interval: Duration,
}

impl Sink {
	pub fn get_name(&self) -> String {
		self.name.clone()
	}
}

#[derive(Deserialize, Serialize, Clone)]
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
				name: s!("post"),
				url: format!("http://localhost:7777/check/{}", tenant),
				batch: rand::thread_rng().gen_range(1, 1000),
				interval: Duration::new(rand::thread_rng().gen_range(1, 300), 0),
			})
			.collect::<Vec<Sink>>();

		Config { tenant, sinks }
	}

	pub fn first_sink(&self) -> &Sink {
		self.sinks.last().unwrap()
	}

	pub fn get_batch_size(&self) -> usize {
		self.sinks.last().unwrap().batch
	}

	pub fn get_interval_as_sec(&self) -> u64 {
		let interval: Duration = self.sinks.last().unwrap().interval;
		interval.as_secs()
	}

	pub fn get_topic(&self, l: &Value) -> String {
		let tenant = get_tenant(l);
		let log_type = get_type(l);
		let destination = self.first_sink().get_name();

		format!("all.{}.{}.{}", destination, log_type, tenant).replace("\"", "")
	}

	pub fn get_general_topic(&self) -> String {
		let tenant = self.tenant.clone();
		let destination = self.first_sink().get_name();

		format!("all.{}.*.{}", destination, tenant).replace("\"", "")
	}

	fn get_count_key(&self) -> String {
		let tenant = self.tenant.clone();
		let destination = self.first_sink().get_name();

		format!("count_{}_{}", tenant, destination).replace("\"", "")
	}
}

fn main() {
	let client =
		redis::Client::open("redis://127.0.0.1:32839/").expect("Cannot open redis connection");
	let mut con = client
		.get_connection()
		.expect("Cannot get redis connection");

	let k_handler =
		kinesis::KinesisHandler::new(s!("test"), None, Some("https://localhost:4568"));
	let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")
		.expect("Cannot connect to RabbitMQ");

	let channel = connection.open_channel(None).expect("Cannot open channel");
	// let exchange = Exchange::direct(&channel);
	let exchange = channel.exchange_declare(ExchangeType::Topic, "topic_logs", ExchangeDeclareOptions::default()).unwrap();
	// let exchange = Exchange::new(&channel, "topic_logs".to_string());

	let receiver = k_handler.get_records_stream(None, None);

	let mut scheduler = Scheduler::new();

	let scheduler_channel = connection.open_channel(None).expect("Cannot open channel");
	let (s, r) = unbounded();

	thread::spawn(move || {
		let exchange = scheduler_channel.exchange_declare(ExchangeType::Topic, "topic_logs", ExchangeDeclareOptions::default()).unwrap();

		let client =
			redis::Client::open("redis://127.0.0.1:32839/").expect("Cannot open redis connection");
		let mut con = client
			.get_connection()
			.expect("Cannot get redis connection");

		loop {
			let conf: Config = r.recv().unwrap();

			let count_key = conf.get_count_key();
			let conf_str = serde_json::to_string(&conf).unwrap();
			let msg = Publish::new(conf_str.as_bytes(), "job");
			exchange.publish(msg).expect("Job published");
			
			let _: () = con.set(count_key, 0).unwrap();
			
		}
	});

	println!("Starting to pool kinesis");
	loop {
		let r = receiver.recv().unwrap();
		let l: Value =
			from_str(str::from_utf8(r.data.as_ref()).unwrap()).expect("JSON cannot be parsed");

		let tenant = get_tenant(&l);
		// println!("Record for tenant: {}", tenant.clone());
		// Log on its shape
		match con.get(tenant.clone()) {
			Ok(c) => {
				let conf_js: String = c;
				let conf: Config = from_str(&conf_js).expect("Cannot parse configuration");
				let topic = conf.get_topic(&l);
				let count_key = conf.get_count_key();
				
				let total: usize = con.incr(count_key.clone(), 1).unwrap();

				
				let l_string = l["metadata"].to_string();
				let message = Publish::new(l_string.as_bytes(), topic.clone());
				// println!("Publishing topic: {}", topic);
				exchange
					.publish(message)
					.expect("Error while publishing a message");
				
				if total >= conf.get_batch_size() {
					let conf_str = serde_json::to_string(&conf).unwrap();
					let msg = Publish::new(conf_str.as_bytes(), "job");
					exchange.publish(msg).expect("Job published");
					
					let _: () = con.set(count_key, 0).unwrap();
				}
			}
			Err(_e) => {
				let conf = Config::new(tenant.clone());
				let conf_str = serde_json::to_string(&conf).unwrap();
				let _: () = con
					.set(tenant.clone(), conf_str.clone())
					.expect("Write to work");

				// let t_list: String = match con.get("tenant_list") {
				// 	Ok(s) => s,
				// 	Err(_) => "".to_string()
				// };

				// let _: () = con
				// 	.set("tenant_list", format!("{},{}", t_list, tenant.clone()))
				// 	.unwrap();

				let _: () = con
					.publish("tenant_config", conf_str)
					.expect("Write to work");
				let s_clone = s.clone();
				
				scheduler.every((conf.get_interval_as_sec() as u32).seconds()).run(move || {
					s_clone.send(conf.clone());
				});

				println!(
					"No config for tenant... {}, just created one, this log is dropped",
					tenant
				);
			}
		}
	}
}
