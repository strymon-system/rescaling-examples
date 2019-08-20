use timely::dataflow::{Scope, Stream, ProbeHandle};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, EmptyConsumerContext};

use dynamic_scaling_mechanism::{Control, ControlInst, BinId};
use timely::dataflow::operators::generic::source;
use rdkafka::message::Message;
use colored::Colorize;

/// Subscribe to the "megaphone-control" topic and return a stream of control commands
pub fn control_stream<G: Scope<Timestamp=usize>>(scope: &mut G, input_probe: ProbeHandle<usize>, widx: usize) -> Stream<G, Control> {

    source(scope, "ControlStream", |mut cap, info| {

        let consumer = if widx == 0 { // Only worker 0 subscribes to the kafka topic
            let topic = "megaphone-control";

            let mut consumer_config = ClientConfig::new();
            consumer_config
                .set("produce.offset.report", "true")
                .set("auto.offset.reset", "smallest")
                .set("group.id", "examples")
                .set("enable.auto.commit", "false")
                .set("enable.partition.eof", "false")
                .set("auto.offset.reset", "earliest")
                .set("session.timeout.ms", "6000")
                .set("bootstrap.servers", &"localhost:9092");

            // Create a Kafka consumer.
            let consumer: BaseConsumer<EmptyConsumerContext> =
                consumer_config.create().expect("Couldn't create consumer");
            consumer.subscribe(&[&topic.to_string()]).expect("Failed to subscribe to topic");

            println!("[W{}@kafka-consumer] subscribed control commands topic", widx);

            Some(consumer)
        } else {
            // only worker 0 keeps a capability
            cap = None;
            None
        };

        let activator = scope.activator_for(&info.address[..]);

        let mut seqno: u64 = 0;

        move |output| {
            if let Some(consumer) = &consumer {
                if let Some(mut cap) = cap.as_mut() {
                    activator.activate(); // we want to be re-scheduled

                    // Poll kafka topic for control commands
                    while let Some(message) = consumer.poll(0) {
                        let message = message.expect("kafka error");
                        let payload = message.payload().expect("null payload");
                        let text = std::str::from_utf8(payload).expect("parse error");

                        // println!("text is {:?}", text);

                        let instructions = text.split(",").map(|text| {
                            let tokens = text.split(" ").map(|x| x.to_lowercase().trim().to_string()).collect::<Vec<_>>();

                            match tokens[0].as_str() {
                                "none" => Some(ControlInst::None),
                                "move" => {
                                    let bin = tokens[1].parse::<usize>().expect("invalid bin for move operation");
                                    let target_worker = tokens[2].parse::<usize>().expect("invalid target worker for move operation");
                                    Some(ControlInst::Move(BinId::new(bin), target_worker))
                                },
                                "map" => {
                                    let map = tokens[1..].into_iter().map(|x| x.parse::<usize>().unwrap()).collect::<Vec<usize>>();
                                    Some(ControlInst::Map(map))
                                }
                                _ => {
                                    println!("{}", "unrecognized command".bold().red());
                                    None
                                }
                            }
                        }).collect::<Vec<_>>();

                        // if command has no syntax error, give it to the control stream
                        if instructions.iter().all(|x| x.is_some()) {
                            let count = instructions.len();
                            let controls = instructions.into_iter().map(move |instr| Control::new(seqno, count, instr.clone().unwrap()));
                            seqno += 1;

                            output.session(cap).give_iterator(controls);
                        }
                    }

                    // Downgrade the capability until we have caught up with the input stream.
                    // What we would like to do: `cap.downgrade(input_probe.time())`.
                    while !input_probe.done() && !input_probe.less_equal(cap.time()) {
                        let new_time = *cap.time() + 1;
                        // println!("downgrading cap to {:?}", new_time);
                        cap.downgrade(&new_time)
                    }

                }

                if input_probe.done() {
                    println!("input stream is done, closing control stream");
                    cap = None;
                }
            }
        }
    })
}
