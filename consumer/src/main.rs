use std::ops::Add;
use std::time::Duration;
use env_logger;
use serde::{Serialize, Deserialize};
use failure::{err_msg, Error};
use futures::{Future, IntoFuture, Stream};
// use lapin_futures as lapin;
use lapin_futures::{Client};
use lapin::{Error as LapErr, BasicProperties, ConnectionProperties};
use lapin::options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions, BasicQosOptions};
use lapin::types::FieldTable;
use log::info;
use futures;
use tokio;
use tokio::runtime::Runtime;
use rand::prelude::Rng;

#[macro_use]
mod utils;


const N_CONSUMERS : u8 = 50;
const N_MESSAGES  : u8 = 5;

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

// fn send_req(job: Job) -> Result {

// }

fn create_consumer(client: &Client, n: u8) -> impl Future<Item = (), Error = ()> + Send + 'static {
    info!("will create consumer {}", n);

    let queue = format!("jobs");
    client.create_channel().and_then(move |channel| {
        info!("creating queue {}", queue);
		channel.basic_qos(50, BasicQosOptions::default())
			.and_then(move |_| {
        		channel.queue_declare(&queue, QueueDeclareOptions::default(), FieldTable::default()).map(move |queue| (channel, queue))
			})
    }).and_then(move |(channel, queue)| {
        info!("creating consumer {}", n);
		let req = reqwest::r#async::Client::builder()
			.timeout(Duration::from_millis(3000))
    		.build().expect("Reqwest client can't be built");
        channel.basic_consume(&queue, "", BasicConsumeOptions::default(), FieldTable::default()).map(move |stream| (req, channel, stream))
    }).and_then(move |(req, channel, stream)| {
        info!("got stream for consumer {}", n);
        stream.and_then(move |message| {
			let job: Job = serde_json::from_str(std::str::from_utf8(&message.data).unwrap()).unwrap();

			req.post(&job.sink.url.replace("\"", ""))
				.body(job.logs.join("\n"))
				.send()
				// .map_err(|e| {
				// 	println!("error: {}", e);
				// 	LapErr::from(lapin::ErrorKind::UnexpectedReply)
				// })
				.then(move |r| {
					match r {
						Ok(r) => {
							match r.status() {
								reqwest::StatusCode::OK => Ok((true, message)),
								_ => Ok((false, message))
							}
						},
						Err(e) => {
							info!("[{}] Error: {}", n, e);
							Ok((false, message))
						}
					}
				})
				// .and_then(move |res| {
				// 	// info!("Woker[{}] Message sent for tenant: {}", n, job.tenant);
				// 	// Ok(message)
				// })
        }).for_each(move |(sent, message)| {
			if sent {
				channel.basic_ack(message.delivery_tag, false)
			}
			else {
				channel.basic_nack(message.delivery_tag, false, true)
			}
		})
    }).map(|_| ()).map_err(move |err| println!("got error in consumer '{}': {}", n, err))
}

fn main() {
    env_logger::init();

    let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    let runtime = Runtime::new().unwrap();

    runtime.block_on_all(
        Client::connect(&addr, ConnectionProperties::default()).map_err(Error::from).and_then(|client| {
            let _client = client.clone();
            futures::stream::iter_ok(0..N_CONSUMERS)
                .for_each(move |n| tokio::spawn(create_consumer(&_client, n)))
                .into_future()
                .map(move |_| client)
                .map_err(|_| err_msg("Couldn't spawn the consumer task"))
        })
		.map_err(|err| eprintln!("An error occured: {}", err))
    ).expect("runtime exited with failure");
}
