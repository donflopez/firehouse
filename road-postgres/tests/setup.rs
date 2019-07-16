use dockers::{
    containers::{ContainerConfig, HostConfig, PortBinding},
    Container, Image,
};
use serde_json::json;
use std::{collections::HashMap, thread, time::Duration};

pub fn run_postgres() {
    let image_name = "postgres";
    let img = Image::pull("postgres".to_owned(), None).expect("Image pulled");

    let mut exposed_ports = HashMap::new();
    exposed_ports.insert(
        "5432/tcp".to_owned(),
        vec![PortBinding {
            HostPort: "5432".to_owned(),
            HostIp: "0.0.0.0".to_owned(),
        }],
    );

    let cont = Container::new(None, Some(image_name.to_owned()))
        .create(
            Some("road_postgres_test".to_owned()),
            Some(ContainerConfig {
                Image: img.Id,
                // ExposedPorts: Some(exposed_ports),
                HostConfig: HostConfig {
                    PortBindings: Some(exposed_ports),
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
        .expect("Cannot create container");

    cont.start().expect("Cannot start container");
}

pub fn stop_postgres() {
    let containers = Container::list().expect("Can't get a list of containers");
    for i in containers {
        if i.Names.len() > 0 && i.Names[0] == "/road_postgres_test" {
            i.kill().expect("Cannot kill the container");
            i.remove().expect("Cannot remove the container");
        }
    }

    let images = Image::list().expect("Can't get a list of images");
    for i in images {
        if i.RepoTags.is_some() {
            let tag = i.RepoTags.clone().unwrap();

            if tag.contains(&"postgres:latest".to_owned()) {
                i.remove().expect("Can't remove image");
            }
        }
    }
}
