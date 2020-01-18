use crossbeam::channel::unbounded;

use rand::RngCore;
use bytes::Bytes;
use rusoto_core::Region;
use rusoto_kinesis::{
	GetRecordsInput, GetRecordsOutput, GetShardIteratorInput, Kinesis, KinesisClient,
	ListShardsInput, PutRecordsInput, PutRecordsRequestEntry, Record, Shard,
};
use std::{str::FromStr, sync::Arc, thread, time::Duration};

#[derive(Clone)]
pub struct KinesisHandler {
	region: Region,
	client: Arc<KinesisClient>,
	stream: String,
}

impl KinesisHandler {
	pub fn new(stream: String, region_name: Option<&str>, kinesis_endpoint: Option<&str>) -> Self {
		let region_name = region_name.unwrap_or("local-stack-1");

		let region = match kinesis_endpoint {
			Some(endpoint) => Region::Custom {
				name: s!(region_name),
				endpoint: s!(endpoint),
			},
			None => Region::from_str(region_name).expect("Cannot parse this region"),
		};

		KinesisHandler {
			client: Arc::new(KinesisClient::new(region.clone())),
			region,
			stream,
		}
	}

	pub fn create_record_from<T>(data: T) -> PutRecordsRequestEntry
	where
		T: serde::Serialize,
	{
		let vec8 = serde_json::to_vec(&data).expect("Failed to parse structure");

		PutRecordsRequestEntry {
			data: Bytes::from(vec8),
			explicit_hash_key: None,
			partition_key: rand::thread_rng().next_u32().to_string(),
		}
	}

	pub fn create_batch_from<T>(&self, data: Vec<T>) -> PutRecordsInput
	where
		T: serde::Serialize,
	{
		PutRecordsInput {
			records: data
				.into_iter()
				.map(KinesisHandler::create_record_from)
				.collect(),
			stream_name: self.stream.clone(),
		}
	}

	pub fn list_shards(
		&self,
	) -> rusoto_core::RusotoFuture<rusoto_kinesis::ListShardsOutput, rusoto_kinesis::ListShardsError>
	{
		self.client.list_shards(ListShardsInput {
			exclusive_start_shard_id: None,
			max_results: None,
			next_token: None,
			stream_creation_timestamp: None,
			stream_name: Some(self.stream.clone()),
		})
	}

	// TODO: Make this return a stream of records
	pub fn get_shard_iterator(
		&self,
		shard_id: String,
		iterator_type: String,
		starting_sequence_number: Option<String>,
	) -> String {
		self.client
			.get_shard_iterator(GetShardIteratorInput {
				shard_id,
				shard_iterator_type: iterator_type,
				starting_sequence_number,
				stream_name: self.stream.clone(),
				timestamp: None,
			})
			.sync()
			.expect("Iterator not found")
			.shard_iterator
			.expect("Shard iterator is empty")
	}

	pub fn put_records(&self, records: PutRecordsInput) {
		// NOTE: Get the output to gather some stats
		self.client
			.put_records(records)
			.sync()
			.expect("Put records failed");
	}

	pub fn get_records(&self, it: &String) -> GetRecordsOutput {
		self.client
			.get_records(GetRecordsInput {
				limit: None,
				shard_iterator: it.to_string(),
			})
			.sync()
			.expect("Failed fetching records")
	}

	pub fn get_records_stream(
		self,
		shard_id: Option<&str>,
		starting_sequence_number: Option<&str>,
	) -> crossbeam::Receiver<Record> {
		let (s, r) = unbounded();

		let (shards, iterator_type) = if shard_id.is_some() {
			let shard = Shard {
				shard_id: shard_id.unwrap().to_owned(),
				..Default::default()
			};
			(vec![shard], s!("AT_SEQUENCE_NUMBER"))
		} else {
			(
				self.list_shards()
					.sync()
					.expect("No shards founds for this stream")
					.shards
					.expect("List of shards not available")
					.clone(),
				s!("LATEST"),
			)
		};

		for shard in shards {
			let this = self.clone();
			let s = s.clone();
			let starting_sequence_number = match starting_sequence_number {
				Some(s) => Some(s.to_string().clone()),
				None => None,
			};
			let iterator_type = iterator_type.clone();
			
			// println!("Shard: {}", shard.shard_id);

			thread::spawn(move || {
				let mut it = this.get_shard_iterator(
					shard.shard_id.clone(),
					iterator_type,
					starting_sequence_number,
				);
				loop {
					let rec = this.get_records(&it);

					it = rec
						.next_shard_iterator
						.expect("Next shard iterator not found")
						.clone();
					let r_len = rec.records.len();
					
					// println!("SHARD ID: {} --- TOTAL RECORDS: {}", shard.shard_id, r_len);
					
					for r in rec.records {
						s.send(r).expect("Couldn't sent the log to the channel");
					}

					if r_len == 0 {
						thread::sleep(Duration::from_millis(500));
					} else {
						thread::sleep(Duration::from_millis(200));
					}
				}
			});
		}

		r
	}

	pub fn put_records_stream<T: 'static>(self) -> crossbeam::Sender<T>
	where
		T: serde::Serialize + Send,
	{
		let (s, r) = unbounded();

		thread::spawn(move || {
			let mut data = Vec::new();

			loop {
				let item = r.recv().unwrap();
				data.push(item);

				if data.len() == 500 {
					let cop = data;
					self.put_records(self.create_batch_from(cop));
					data = Vec::new();
				}
			}
		});

		s
	}
}
