#[macro_use]
extern crate serde_derive;
extern crate amiquip;
extern crate bytes;
extern crate redis;
extern crate reqwest;
extern crate serde_yaml;

use amiquip::{
	Connection, ConsumerMessage, ConsumerOptions, Exchange, FieldTable, Publish,
	QueueDeclareOptions, ExchangeType, ExchangeDeclareOptions
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

	let channel = connection.open_channel(Some(rand::thread_rng().gen_range(1, 50)))?;
	channel.qos(
		0,
		100,
		false,
	)?;
	println!("Creating thread");
	thread::spawn(move || {
		let mut msg_vec: Vec<String> = Vec::new();
		let topic = format!("all.*.*.{}", t).replace("\"", "");

		let exchange = channel.exchange_declare(ExchangeType::Topic, "topic_logs", ExchangeDeclareOptions::default()).unwrap();

		let queue = channel
			.queue_declare("", QueueDeclareOptions {
				exclusive: true,
				..QueueDeclareOptions::default()
				})
			.expect("Cannot declare queue");

		// channel.queue_bind(queue.name(), "topic_logs", topic, FieldTable::default()).unwrap();
		queue.bind(&exchange, topic, FieldTable::new()).unwrap();

		// let consumer = channel.basic_consume(queue.name(), ConsumerOptions {
		// 	exclusive: true,
		// 	..ConsumerOptions::default()
		// 	}).unwrap();

		let consumer = queue.consume(ConsumerOptions::default()).unwrap();

		println!("Ready to get messages");
		loop {
			match consumer.receiver().recv_timeout(Duration::from_secs(conf.sinks[0].interval.as_secs())) {
				Ok(msg) => {
					match msg {
						ConsumerMessage::Delivery(delivery) => {
							let client = Client::new();
							let body: String = String::from_utf8_lossy(&delivery.body).to_string();

							if msg_vec.len() == conf.sinks[0].batch - 1 {
								msg_vec.push(body.clone());
								println!("Sending batch for tenant {}", tenant.clone());
								match client
									.post(&conf.sinks[0].url.replace("\"", ""))
									.body(msg_vec.join("\n").to_string())
									.send()
								{
									Err(e) => println!("POST ERROR: {}", e),
									_ => println!("Batch sent ok"),
								};

								msg_vec.clear();
							} else {
								msg_vec.push(body);
							}

							// println!("Received [{}]", body);
							consumer.ack(delivery).unwrap();
						}
						other => break
					}
				}
				_ => {
					println!("Timeout reached, cleaning batch and running again! {}", msg_vec.len());
					if msg_vec.len() > 0 {
						let client = Client::new();
						match client
								.post(&conf.sinks[0].url.replace("\"", ""))
								.body(msg_vec.join("\n").to_string())
								.send()
							{
								Err(e) => println!("POST ERROR: {}", e),
								_ => println!("Batch sent ok"),
							};

							msg_vec.clear();
					}
				}
			}
		}
		// for message in consumer.receiver().iter() {
		// 	match message {
		// 		ConsumerMessage::Delivery(delivery) => {

		// 			let body: String = String::from_utf8_lossy(&delivery.body).to_string();

		// 			if msg_vec.len() == conf.sinks[0].batch - 1 {
		// 				msg_vec.push(body.clone());
		// 				println!("Sending batch for tenant {}", tenant.clone());
		// 				match client
		// 					.post(&conf.sinks[0].url.replace("\"", ""))
		// 					.body(msg_vec.join("\n").to_string())
		// 					.send()
		// 				{
		// 					Err(e) => println!("POST ERROR: {}", e),
		// 					_ => println!("Batch sent ok"),
		// 				};

		// 				msg_vec.clear();
		// 			} else {
		// 				msg_vec.push(body);
		// 			}

		// 			// println!("Received [{}]", body);
		// 			consumer.ack(delivery).unwrap();
		// 		}
		// 		other => {
		// 			println!("Consumer ended: {:?}", other);
		// 			break;
		// 		}
		// 	}
		// }
	});

	Ok(connection)
}

fn main() {
	let client =
		redis::Client::open("redis://127.0.0.1:32839/").expect("Cannot open redis connection");
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
	println!("Tenant list {:?}", t_list_vec);
	for tenant in t_list_vec {
		if tenant.len() > 0 {
			println!("Adding tenant {}", tenant.clone());
			let conf: String = con.get(tenant.clone()).unwrap();
			let conf: Config = serde_json::from_str(&conf).unwrap();

			connection = run_queue(connection, tenant.clone(), conf).unwrap();
		}
	}

	loop {
		let msg = redis_pubsub.get_message().unwrap();
		let payload: String = msg.get_payload().unwrap();
		
		println!("Adding new tenant by subscription: {}", payload.clone());

		let conf: Config = serde_json::from_str(&payload).unwrap();

		connection = run_queue(connection, conf.tenant.clone(), conf).unwrap();
	}
}
