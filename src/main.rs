use mqtt311::Publish;
use rumqtt::{MqttClient, MqttOptions, Notification, QoS, Receiver};
use std::collections::HashMap;

use std::{sync::mpsc, thread};

type ClientId = u16;

#[derive(Clone)]
struct Destination {
    topic: Topic,
    client_id: ClientId,
    qos: QoS,
}

struct PayloadForward {
    destinations: Vec<Destination>,
    payload: Vec<u8>,
}

trait PayloadRouter {
    fn route_packet(&self, incoming_packet: &Publish) -> Option<PayloadForward>;
}

#[derive(Clone)]
struct Topic {
    path: Vec<String>,
}

enum TopicCompare {
    Mismatch,
    Match { additional_path: Vec<String> },
}

impl Topic {
    fn to_string(&self) -> String {
        self.path.join("/")
    }
    fn accepts(&self, other: &Self) -> TopicCompare {
        println!("CMP {:?} {:?}", self.path, other.path);
        for (i, part) in self.path.iter().enumerate() {
            if part == "#" {
                return TopicCompare::Match {
                    additional_path: other.path[i..].to_vec(),
                };
            } else if part != "+" && *part != other.path[i] {
                return TopicCompare::Mismatch;
            }
        }
        if self.path.len() == other.path.len() {
            TopicCompare::Match {
                additional_path: vec![],
            }
        } else {
            TopicCompare::Mismatch
        }
    }
}

impl From<&str> for Topic {
    fn from(s: &str) -> Self {
        Topic {
            path: s.split("/").map(|s| s.to_string()).collect(),
        }
    }
}

struct SingleBasicRoute {
    source_client_id: ClientId,
    source_topic: Topic,
    dest_client_id: ClientId,
    dest_topic: Topic,
    dest_qos: QoS,
}

enum SingleBasicRouteError {
    NotEnoughArguments { required: u8, given: u8 },
    UnknownClient(String),
    InvalidQos(mqtt311::Error),
    UnparsableQos(std::num::ParseIntError),
}

impl SingleBasicRoute {
    fn from_string(
        clients_list: HashMap<String, ClientId>,
        s: &str,
    ) -> Result<SingleBasicRoute, SingleBasicRouteError> {
        let words: Vec<_> = s.split(" ").collect();
        let min_arg_nb = 2 + 3;
        if words.len() < min_arg_nb as usize {
            return Err(SingleBasicRouteError::NotEnoughArguments {
                required: min_arg_nb,
                given: words.len() as u8, // TODO Clamp this instead
            });
        }

        let source_client_id = match clients_list.get(words[0]) {
            Some(id) => id.clone(),
            None => return Err(SingleBasicRouteError::UnknownClient(words[0].to_string())),
        };
        let source_topic = Topic::from(words[1]);

        let dest_client_id = match clients_list.get(words[2]) {
            Some(id) => id.clone(),
            None => return Err(SingleBasicRouteError::UnknownClient(words[2].to_string())),
        };
        let dest_topic = Topic::from(words[3]);
        let dest_qos = match words[4].parse::<u8>() {
            Ok(n) => match QoS::from_u8(n) {
                Ok(qos) => qos,
                Err(e) => return Err(SingleBasicRouteError::InvalidQos(e)),
            },
            Err(e) => return Err(SingleBasicRouteError::UnparsableQos(e)),
        };

        Ok(SingleBasicRoute {
            source_client_id,
            source_topic,
            dest_client_id,
            dest_topic,
            dest_qos,
        })
    }
}

#[derive(Clone)]
struct BasicRoute {
    source_topic: Topic,
    dests: Vec<Destination>,
}

#[derive(Clone)]
struct BasicRouter {
    routes: Vec<BasicRoute>,
}

impl PayloadRouter for BasicRouter {
    fn route_packet(&self, incoming_packet: &Publish) -> Option<PayloadForward> {
        let in_topic = Topic::from(incoming_packet.topic_name.as_str());
        let mut destinations = vec![];
        for route in self.routes.iter() {
            let dest_topic = match route.source_topic.accepts(&in_topic) {
                TopicCompare::Mismatch => None,
                TopicCompare::Match { additional_path } => Some(additional_path),
            };
            if let Some(dest_topic) = dest_topic {
                for dest in route.dests.iter() {
                    let topic = Topic {
                        path: [&dest.topic.path[..], &dest_topic].concat().to_vec(),
                    };
                    destinations.push(Destination {
                        topic,
                        client_id: dest.client_id,
                        qos: dest.qos,
                    });
                }
            }
        }
        println!("incoming_packet {:?}", incoming_packet);
        if destinations.len() > 0 {
            Some(PayloadForward {
                destinations,
                payload: (*incoming_packet.payload).clone(),
            })
        } else {
            None
        }
    }
}

struct Listener {
    router: BasicRouter,
    receiver: Receiver<Notification>,
    emitters_channel: mpsc::Sender<PayloadForward>,
}

impl Listener {
    fn start(self) {
        thread::spawn(move || {
            self.run();
        });
    }

    fn run(&self) {
        for notification in self.receiver.iter() {
            println!("NOT: {:?}", notification);
            match notification {
                Notification::Publish(publish) => {
                    if let Some(forward) = self.router.route_packet(&publish) {
                        match self.emitters_channel.send(forward) {
                            Err(e) => println!("Packet forward failed: {:?}", e),
                            _ => (),
                        }
                    }
                }
                _ => (),
            }
        }
    }
}

struct Emitters {
    listeners_channel: mpsc::Receiver<PayloadForward>,
    clients: Vec<MqttClient>,
}

impl Emitters {
    fn start(mut self) {
        thread::spawn(move || {
            self.run();
        });
    }

    fn run(&mut self) {
        for forward in self.listeners_channel.iter() {
            for dest in forward.destinations.iter() {
                match self.clients[dest.client_id as usize].publish(
                    dest.topic.to_string(),
                    dest.qos,
                    false,
                    &forward.payload[..],
                ) {
                    Err(e) => println!("Publish failed: {:?}", e),
                    _ => (),
                }
            }
        }
    }
}

struct BridgeConfiguration {
    mqtt_options: Vec<MqttOptions>,
    routes: Vec<Vec<BasicRoute>>,
}

struct Bridge {}

impl Bridge {
    fn start(conf: &BridgeConfiguration) {
        // Build channel
        let (sender, receiver) = mpsc::channel();

        // Build MQTT clients
        let (mut clients, mqtt_receivers): (Vec<_>, Vec<_>) = conf
            .mqtt_options
            .iter()
            .map(|options| MqttClient::start(options.clone()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|e| panic!("Mqtt client creation: {:?}", e))
            .iter()
            .cloned()
            .unzip();

        mqtt_receivers
            .into_iter()
            .enumerate()
            .for_each(|(i, receiver)| {
                conf.routes[i].iter().for_each(|route| {
                    println!("SUB {}", route.source_topic.to_string());
                    match clients[i].subscribe(route.source_topic.to_string(), QoS::AtLeastOnce) {
                        Ok(()) => (),
                        Err(e) => panic!("{:?}", e),
                    }
                });
                Listener {
                    router: BasicRouter {
                        routes: conf.routes[i].clone(),
                    },
                    receiver,
                    emitters_channel: sender.clone(),
                }
                .start();
            });

        Emitters {
            listeners_channel: receiver,
            clients,
        }
        .run();
    }
}

fn main() {
    Bridge::start(&BridgeConfiguration {
        mqtt_options: vec![
            MqttOptions::new("test1", "localhost", 1883),
            MqttOptions::new("test2", "localhost", 1883),
            MqttOptions::new("test3", "localhost", 1883),
        ],
        routes: vec![
            vec![BasicRoute {
                source_topic: Topic::from("/test1/input/#"),
                dests: vec![
                    Destination {
                        client_id: 1,
                        topic: Topic::from("/test2/output/test1"),
                        qos: QoS::AtLeastOnce,
                    },
                    Destination {
                        client_id: 2,
                        topic: Topic::from("/test3/output/test1"),
                        qos: QoS::AtLeastOnce,
                    },
                ],
            }],
            vec![],
            vec![],
        ],
    });
}
