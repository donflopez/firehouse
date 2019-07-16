#[macro_use]
extern crate serde_derive;
extern crate amiquip;
extern crate bytes;
extern crate redis;
extern crate reqwest;
extern crate serde_yaml;

use amiquip::{
	Connection, ConsumerMessage, ConsumerOptions, Exchange, FieldTable, Publish,
	QueueDeclareOptions,
};
use rand::prelude::*;
use redis::Commands;
use reqwest::Client;
use serde_json::{from_str, Value};
use std::time::Duration;
use std::{collections::VecDeque, str, thread};

#[macro_use]
mod utils;
mod kinesis;

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
				url: format!("http://localhost/check/{}", tenant),
				batch: rand::thread_rng().gen_range(1, 1000),
				interval: Duration::new(rand::thread_rng().gen_range(1, 300), 0),
			})
			.collect::<Vec<Sink>>();

		Config { tenant, sinks }
	}
}

fn run_queue(
	mut connection: Connection,
	tenant: String,
	conf: Config,
) -> std::result::Result<Connection, amiquip::Error> {
	let t = tenant.clone();

	let channel = connection.open_channel(None)?;
	channel.qos(
		conf.sinks[0].batch as u32,
		conf.sinks[0].batch as u16,
		false,
	)?;

	thread::spawn(move || {
		let mut msg_vec: Vec<String> = Vec::new();
		let queue = channel
			.queue_declare(format!("all.{}.*.*", t), QueueDeclareOptions::default())
			.expect("Cannot declare queue");

		let consumer = queue.consume(ConsumerOptions::default()).unwrap();
		let client = Client::new();

		for message in consumer.receiver().iter() {
			match message {
				ConsumerMessage::Delivery(delivery) => {
					let body: String = String::from_utf8_lossy(&delivery.body).to_string();

					if msg_vec.len() == conf.sinks[0].batch - 1 {
						msg_vec.push(body);

						match client
							.post(&conf.sinks[0].url)
							.body(msg_vec.join("\n"))
							.send()
						{
							Err(e) => println!("POST ERROR: {}", e),
							_ => (),
						};

						msg_vec.clear();
					} else {
						msg_vec.push(body);
					}

					// println!("Received [{}]", body);
					consumer.ack(delivery).unwrap();
				}
				other => {
					println!("Consumer ended: {:?}", other);
					break;
				}
			}
		}
	});

	Ok(connection)
}

fn main() {
	let client =
		redis::Client::open("redis://127.0.0.1:32768/").expect("Cannot open redis connection");
	let mut con = client
		.get_connection()
		.expect("Cannot get redis connection");
	let mut con2 = client
		.get_connection()
		.expect("Cannot get redis connection");

	let mut redis_pubsub = con2.as_pubsub();

	let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")
		.expect("Cannot connect to RabbitMQ");

	let t_list: String = con.get("tenant_list").expect("Not tenant_list available");

	redis_pubsub.subscribe("tenant_config").unwrap();
	let t_list_vec = t_list.split(',').into_iter().map(|s| s.to_string());

	for tenant in t_list_vec {
		println!("Adding tenant {}", tenant.clone());
		let conf: String = con.get(tenant.clone()).unwrap();
		let conf: Config = serde_json::from_str(&conf).unwrap();

		connection = run_queue(connection, tenant.clone(), conf).unwrap();
	}

	loop {}
}
