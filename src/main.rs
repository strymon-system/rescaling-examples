use std::collections::HashMap;

use timely::dataflow::{InputHandle, ProbeHandle, Scope, Stream};
use timely::dataflow::operators::{Map, Operator, Inspect, Probe};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::aggregation::StateMachine;
use dynamic_scaling_mechanism::state_machine::BinnedStateMachine;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::io::{BufReader, BufRead};
use dynamic_scaling_mechanism::{Control, ControlInst, BinId, BIN_SHIFT};
use timely::dataflow::operators::broadcast::Broadcast;
use timely::ExchangeData;
use timely::dataflow::operators::exchange::Exchange as FuckOff;
use colored::Colorize;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let mut lines_in = InputHandle::new();
        let mut control_in = InputHandle::new();

        let mut stateful_probe = ProbeHandle::new();
        let mut correct_probe = ProbeHandle::new();
        let mut verify_probe = ProbeHandle::new();

        let widx = worker.index();

        worker.dataflow::<usize, _, _>(|scope| {
            let control = control_in.to_stream(scope).broadcast();

            control.inspect(|c| println!("{}", format!("control message is {:?}", c).bold().yellow()));

            let words_in =
                lines_in
                    .to_stream(scope)
                    .flat_map(|text: String|
                        text.split_whitespace()
                            .map(move |word| (word.to_owned(), 1))
                            .collect::<Vec<_>>()
                    );

            let stateful_out =
                words_in
                    .stateful_state_machine(|key: &String, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((key.clone(), *agg)))
                    }, |key| calculate_hash(key), &control)
                    .inspect(move |x| println!("[W{}] stateful seen: {:?}", widx, x))
                    .probe_with(&mut stateful_probe);

            let correct =
                words_in
                    .state_machine(|key: &String, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((key.clone(), *agg)))
                    }, |_key| 0) // TODO need to send everything to worker 0 as number of peers changes and it would compute the wrong answer (no routing table without megaphone)
                    .inspect(move |x| println!("[W{}] correct seen: {:?}", widx, x))
                    .probe_with(&mut correct_probe);

            verify(&correct.exchange(|_| 0), &stateful_out.exchange(|_| 0)).probe_with(&mut verify_probe);
        });

        if worker.bootstrap() { return; }

        if worker.index() == 0 {
            let controls = vec![
                vec![ControlInst::Move(BinId::new(0), 1), ControlInst::Move(BinId::new(1), 0)],
                vec![ControlInst::None], // make sure new worker has correct map
                // vec![ControlInst::Move(BinId::new(0), 2), ControlInst::Move(BinId::new(1), 2)],
                vec![ControlInst::Map(vec![2; 1 << BIN_SHIFT])],
            ];
            let mut controls =
                controls
                    .iter()
                    .enumerate()
                    .map(|(seqno, instructions)|
                        instructions.into_iter().map(|instr| Control::new(seqno as u64, instructions.len(), instr.clone())).collect::<Vec<_>>()
                    );

            let reader = BufReader::new(File::open("text/sample.txt").unwrap());
            let mut lines = reader.lines();

            let batch_size = 10;

            let mut round = 0;

            loop {
                let mut done = false;

                for _ in 0..batch_size {
                    if let Some(line) = lines.next() {
                        if widx == 0 {
                            lines_in.send(line.unwrap());
                            std::thread::sleep(std::time::Duration::from_millis(500))
                        }
                        lines_in.advance_to(round + 1);
                        control_in.advance_to(round + 1);
                        worker.step_while(|| stateful_probe.less_than(lines_in.time()));
                        worker.step_while(|| correct_probe.less_than(lines_in.time()));
                        round += 1;
                    } else {
                        done = true;
                    }
                }

                if let Some(controls) = controls.next() {
                    for control in controls {
                        control_in.send(control)
                    }

                    lines_in.advance_to(round + 1);
                    control_in.advance_to(round + 1);
                    worker.step_while(|| stateful_probe.less_than(lines_in.time()));
                    worker.step_while(|| correct_probe.less_than(lines_in.time()));
                    round += 1;
                }

                if done { break }
            }
        }
    }).unwrap();
}

fn verify<S: Scope, T: ExchangeData + Ord + ::std::fmt::Debug>(correct: &Stream<S, T>, output: &Stream<S, T>) -> Stream<S, ()> {
    let mut in1_pending: HashMap<_, Vec<_>> = Default::default();
    let mut in2_pending: HashMap<_, Vec<_>> = Default::default();
    let mut data_buffer: Vec<T> = Vec::new();
    correct.binary_notify(&output, Exchange::new(|_| 0), Exchange::new(|_| 0), "Verify", vec![],
                          move |in1, in2, _out, not| {
                              in1.for_each(|time, data| {
                                  data.swap(&mut data_buffer);
                                  in1_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data_buffer.drain(..));
                                  not.notify_at(time.retain());
                              });
                              in2.for_each(|time, data| {
                                  data.swap(&mut data_buffer);
                                  in2_pending.entry(time.time().clone()).or_insert_with(Default::default).extend(data_buffer.drain(..));
                                  not.notify_at(time.retain());
                              });
                              not.for_each(|time, _, _| {
//                                  println!("comparing time {:?}\n\tin1 = {:?}\n\tin2 = {:?}", *time.time(), in1_pending, in2_pending);
                                  let mut v1 = in1_pending.remove(time.time()).unwrap_or_default();
                                  let mut v2 = in2_pending.remove(time.time()).unwrap_or_default();
                                  v1.sort();
                                  v2.sort();
                                  assert_eq!(v1.len(), v2.len());
                                  let i1 = v1.iter();
                                  let i2 = v2.iter();
                                  for (a, b) in i1.zip(i2) {
                                      assert_eq!(a, b, " at {:?}", time.time());
                                  }
                              })
                          },
    )
}

