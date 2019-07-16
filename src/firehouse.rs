use serde_yaml::Value;
use std::fs::File;

use crate::kinesis::KinesisHandler;

pub struct Firehose {
    config: FirehoseConfig,
}

#[derive(Serialize, Deserialize)]
pub struct FirehoseConfig {
    input: Value,
    output: Value,
    aliases: Value,
}

// impl Firehose {
//     fn new(config: String) -> Self {
//         let config =
//             serde_yaml::from_str(config.as_str()).expect("We couldn't parse this YAML file");
//         Firehose { config }
//     }

//     fn init() {}

//     fn fetch_from(
//         self,
//         stream: &str,
//         region_name: Option<&str>,
//         kinesis_endpoint: Option<&str>,
//     ) -> Self {
//         let ks = KinesisHandler::new(s!(stream), region_name, kinesis_endpoint);

//         ks.get_records_stream();
//     }
// }
