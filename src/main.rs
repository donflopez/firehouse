#[macro_use]
extern crate serde_derive;
extern crate amiquip;
extern crate bytes;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;
extern crate redis;
extern crate serde_yaml;

use std::ops::Add;
use lapin::{
  BasicProperties, Channel, Connection, ConnectionProperties, ConsumerDelegate,
  message::Delivery,
  options::*,
  types::FieldTable,
};
use rand::prelude::*;
use redis::Commands;
use serde_json::{from_str, Value};
use std::{str, thread, collections::HashMap};
use std::time::Duration;
use sled::{Db, IVec, Result as SledResult};
// Scheduler, and trait for .seconds(), .minutes(), etc.
use clokwerk::{Scheduler, TimeUnits};
// Import week days and WeekDay
use clokwerk::Interval::*;
#[macro_use]
use crossbeam::channel::*;

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
	total: usize,
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
				interval: Duration::new(rand::thread_rng().gen_range(10, 300), 0),
			})
			.collect::<Vec<Sink>>();

		Config { tenant, sinks, total: 0 }
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

#[derive(Deserialize, Serialize, Clone)]
struct Job {
	tenant: String,
	sink: Sink,
	logs: Vec<String>
}

impl Job {
	pub fn new(tenant: String, sink: Sink) -> Self {
		Job {
			tenant,
			sink,
			logs: Vec::new()
		}
	}

	pub fn add(&mut self, log: String) {
		self.logs.push(log);
	}
}


fn ivec_to_string(res: IVec) -> String {
	String::from_utf8(res.to_vec()).expect("to_slice string result cannot be parsed")
}

fn main() {
	let tree = Db::start_default(std::path::Path::new("store")).expect("Cannot open db");

	let k_handler =
		kinesis::KinesisHandler::new(s!("flopez-poc-logs-streaming"), Some("eu-west-3"), None);

	let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
	let conn = Connection::connect(&addr, ConnectionProperties::default()).wait().expect("connection error");
	let channel = conn.create_channel().wait().expect("create_channel");

	let receiver = k_handler.get_records_stream(None, None);

	let mut counter = 0;
	println!("Starting to pool kinesis");
	let mut jobs: HashMap<String, Job> = HashMap::new();

	loop {
		select! {
			default(Duration::from_millis(500)) => {
				for (i, j) in jobs.into_iter() {
					let str_job = serde_json::to_vec(&j).expect("Cannot parse job");

					channel.basic_publish("", "jobs", BasicPublishOptions::default(), str_job, BasicProperties::default()).wait().expect("basic_publish");
				}

				BasicPublishOptions {

				};

				jobs = HashMap::new();
				println!("Timeout, counter: {}", counter);
			},
			recv(receiver) -> r => {
				let r = r.unwrap();
				let l: Value =
					from_str(str::from_utf8(r.data.as_ref()).unwrap()).expect("JSON cannot be parsed");

				counter += 1;

				let tenant = get_tenant(&l);
				// println!("Record for tenant: {}", tenant.clone());
				// Log on its shape
				match tree.get(tenant.clone()) {
					Ok(res) => 
						match res {
							Some(c) => {
								let conf: Config = serde_json::from_str(&ivec_to_string(c)).expect("Cannot parse conf");
								// println!("Adding log for tenant: {}", tenant.clone());
								let job: &mut Job = match jobs.get_mut(&tenant) {
									Some(job) => job,
									None => {
										let j = Job::new(tenant.clone(), conf.first_sink().clone());
										jobs.insert(tenant.clone(), j.clone());
										jobs.get_mut(&tenant).unwrap()
									}
								};

								let l_string = l["metadata"].to_string();
								job.add(l_string);

								// jobs.insert(tenant.clone(), job.to_owned());
							}
							None => {
								let conf = Config::new(tenant.clone());
								let conf_str = serde_json::to_vec(&conf).expect("stringify doesnt work for config");
								let mut job = Job::new(tenant.clone(), conf.first_sink().clone());

								let l_string = l["metadata"].to_string();

								job.add(l_string);

								jobs.insert(tenant.clone(), job);

								tree.set(tenant.clone(), conf_str).unwrap();
								println!(
									"({}) No config for tenant... {}, just created one, this log is dropped",
									counter,
									tenant
								);

							}
						}
					Err(_e) => {
						panic!("WTF?!");
					}
				}		
			}
		}
	}
}
