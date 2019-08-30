extern crate clap;
extern crate fnv;
extern crate rand;
extern crate timely;
extern crate streaming_harness;
extern crate hdrhist;
extern crate dynamic_scaling_mechanism;
extern crate abomonation;

use std::alloc::System;

#[global_allocator]
static GLOBAL: System = System;

use std::cell::RefCell;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use streaming_harness::util::ToNanos;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Broadcast, Operator, Probe};

use timely::dataflow::channels::pact::Pipeline;

use dynamic_scaling_mechanism::{Control, ControlInst, BinId, BIN_SHIFT};
use dynamic_scaling_mechanism::state_machine::BinnedStateMachine;

use timely::dataflow::operators::input::Handle;
use rescaling_examples::{verify, LoadBalancer, LinesGenerator};
use timely::dataflow::operators::inspect::Inspect;
use std::process::Command;
use std::fs::File;
use colored::Colorize;
use std::collections::VecDeque;
use std::io::Write;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::exchange::Exchange;

const WORKER_BOOTSTRAP_MARGIN: u64 = 500_000_000; // wait 500 millis after spawning before sending move commands

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h: ::fnv::FnvHasher = Default::default();
    t.hash(&mut h);
    h.finish()
}

fn main() {
    let rate: u64 = 10_00;
    let duration_ns: u64 = 40*1_000_000_000;
    let validate = false;
    let key_space = 1000;
    let words_per_line = 100;
    let word_length = 10;

    let timelines: Vec<_> = timely::execute_from_args(std::env::args(), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        // Declare re-used input, control and probe handles.
        let mut input: Handle<_, ()> = InputHandle::new();
        let mut control_input = InputHandle::new();
        // let mut control_input_2 = InputHandle::new();
        let mut probe = ProbeHandle::new();
        let probe2 = ProbeHandle::clone(&mut probe);

        // Generate the times at which input should be produced
        let input_times = || streaming_harness::input::ConstantThroughputInputTimes::<u64, u64>::new(
            1, 1_000_000_000 / rate, duration_ns);

        let mut input_times_gen =
            ::streaming_harness::input::SyntheticInputTimeGenerator::new(input_times());

        let element_hdr = Rc::new(RefCell::new(::hdrhist::HDRHist::new()));
        let element_hdr2 = Rc::clone(&element_hdr);

        // Construct the dataflow
        worker.dataflow(|scope: &mut ::timely::dataflow::scopes::Child<_, usize>| {
            let control = control_input.to_stream(scope).broadcast();
            control.inspect(move |c| println!("[W{}] {}", index, format!("control message is {:?}", c).bold().yellow()));

            // Construct the data generator
            let lines = input
                .to_stream(scope)
                .unary_frontier(Pipeline, "Data generator", |mut cap, _info| {
                    let mut lines_generator = LinesGenerator::new(key_space, words_per_line, word_length);
                    let mut last_production_time = 0;

                    move |input, output| {
                        // Input closed, we're done
                        if input.frontier().is_empty() {
                            cap.take();
                        } else if let Some(cap) = cap.as_mut() {
                            let current_time = input.frontier().frontier()[0];
                            let probe_time = probe2.with_frontier(|f| if f.is_empty() { 0 } else { f[0] });
                            let delta_probe = current_time - probe_time;
                            let delta_production = current_time - last_production_time;
                            // if delta to probe is smaller than half of delta to production, consider to produce more data
                            if delta_probe <= delta_production * 2 {
                                if let Some(mut it) = input_times_gen.iter_until((current_time) as u64) {
                                    // `it` is some => we are still running!
                                    // If there are actual elements to be produced, open a session and produce them
                                    if let Some(_) = it.next() {
                                        let mut session = output.session(cap);
                                        session.give(lines_generator.next());
                                        let mut word_count = words_per_line;
                                        for _t in it {
                                            session.give(lines_generator.next());
                                            word_count += words_per_line;
                                        }
                                        element_hdr2.borrow_mut().add_value(word_count as u64);
                                        last_production_time = current_time;
                                    }
                                }
                                cap.downgrade(&current_time);
                            }
                        }
                    }
                });

            let rr = RefCell::new(0);

            let words =
                lines
                    .exchange(move |_| { let mut rr = rr.borrow_mut(); *rr+=1; *rr }) // round-robin
                    .flat_map(|text: String|
                        text.split_whitespace()
                            .map(move |word| (word.to_owned(), 1))
                            .collect::<Vec<_>>()
                    );

            let sst_output =
                words
                    .stateful_state_machine(|key: &String, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((key.clone(), *agg)))
                    }, |key| calculate_hash(key), &control)
                    //.inspect(move |x| println!("{:?}", x))
                    .probe_with(&mut probe);

            if validate {
                use timely::dataflow::operators::aggregation::StateMachine;
                let correct = words
                    .state_machine(|key: &String, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((key.clone(), *agg)))
                    }, |_key| 0); // plain exchange won't compute correct counts when after rescaling (no routing table)
                verify(&sst_output, &correct).probe_with(&mut probe);
            }
        });

        if worker.bootstrap() { return None; }

        let mut spawn_at_times: VecDeque<u64> = VecDeque::new();
        spawn_at_times.push_back(duration_ns/3);
        spawn_at_times.push_back(2*duration_ns/3);

        let mut spawn_metrics = Vec::new();

        let mut output_metric_collector =
            ::streaming_harness::output::default::hdrhist_timeline_collector(
                input_times(),
                0, 2_000_000_000, duration_ns - 2_000_000_000, duration_ns,
                250_000_000);

        let n = std::env::var("N").expect("missing N env var -- number of processes").parse::<usize>().unwrap();
        let w = std::env::var("W").expect("missing W env var -- number of workers").parse::<usize>().unwrap();
        let mut p = n;
        let mut nn = n+1;
        let mut join = 0;
        let mut spawn_info = None;

        let mut load_balancer = LoadBalancer::new((0..peers).collect(), 1 << BIN_SHIFT);

        let mut input = Some(input);
        let mut control_input = Some(control_input);
        let mut control_sequence = 0;

        let timer = ::std::time::Instant::now();

        loop {
            if index != 0 {
                input.take().unwrap().close();
                control_input.take().unwrap().close();
                break;
            }

            let elapsed_ns = timer.elapsed().to_nanos();

            if let Some(spawn_at_time) = spawn_at_times.front() {
                if elapsed_ns >= *spawn_at_time && spawn_info.is_none() {
                    spawn_at_times.pop_front();

                    let old_peers = worker.peers();

                    let stdout = File::create(format!("/tmp/process-{}-stdout", p)).unwrap();
                    let stderr = File::create(format!("/tmp/process-{}-stderr", p)).unwrap();

                    Command::new("cargo")
                        .stdout(stdout)
                        .stderr(stderr)
                        .arg("run")
                        .arg("--bin")
                        .arg("benchmark")
                        .arg("--")
                        .arg("-n")
                        .arg(n.to_string())
                        .arg("-w")
                        .arg(w.to_string())
                        .arg("-p")
                        .arg(p.to_string())
                        .arg("--join")
                        .arg(join.to_string())
                        .arg("--nn")
                        .arg(nn.to_string())
                        .spawn()
                        .expect("failed to spawn new process");

                    // wait for the new worker to join the cluster
                    while old_peers == worker.peers() {
                        worker.step();
                    }

                    (0..w)
                        .map(|i| ControlInst::Bootstrap(join, p*w+i))
                        .map(|cmd| Control::new(control_sequence, w, cmd))
                        .for_each(|ctrl| control_input.as_mut().unwrap().send(ctrl));

                    control_sequence += 1;

                    assert!(spawn_info.is_none());
                    spawn_info = Some((p, elapsed_ns));

                    p += 1;
                    nn += 1;
                    join += 1;
                    join %= worker.peers();
                }
            }

            let mut bin_moved = false;
            if let Some((new_process, bootstrap_time)) = spawn_info {
                if elapsed_ns > bootstrap_time + WORKER_BOOTSTRAP_MARGIN {
                    // move about 1/peers of the bins to the new `w` workers
                    let new_workers = (new_process*w..new_process*w+w).collect::<Vec<_>>();
                    let moves = load_balancer.add_workers(new_workers).map(|(bin, to)| ControlInst::Move(BinId::new(bin), to)).collect::<Vec<_>>();
                    let count = moves.len();
                    moves
                        .into_iter()
                        .map(|mv| Control::new(control_sequence, count, mv))
                        .for_each(|ctrl| control_input.as_mut().unwrap().send(ctrl));

                    control_sequence += 1;
                    bin_moved = true;

                    println!("bootstrap worker:\tbootstrap={}\tmoves={}", bootstrap_time, elapsed_ns);
                    spawn_metrics.push((bootstrap_time, elapsed_ns));
                }
            }
            if bin_moved { spawn_info = None; }

            output_metric_collector.acknowledge_while(
                elapsed_ns,
                |t| {
                    !probe.less_than(&(t as usize)) // TODO(lorenzo) +1 ?
                });

            if input.is_none() {
                break;
            }

            if elapsed_ns < duration_ns {
                let input = input.as_mut().unwrap();
                input.advance_to(elapsed_ns as usize);
                if let Some(control_input) = control_input.as_mut() {
                    if *control_input.time() < elapsed_ns as usize {
                        control_input.advance_to(elapsed_ns as usize);
                    }
                }
            } else {
                input.take().unwrap();
                control_input.take();
            }

            if input.is_some() {
                worker.step();
            } else {
                while worker.step() { }
            }
        }

        let element_hdr = element_hdr.borrow();
        for (value, prob, count) in element_hdr.ccdf() {
            println!("count_ccdf\t{}\t{}\t{}", value, prob, count);
        }

        if index == 0 { Some((output_metric_collector.into_inner(), spawn_metrics)) } else { None }

    }).expect("unsuccessful execution").join().into_iter().map(|x| x.unwrap()).collect();

    let result: Vec<(streaming_harness::timeline::Timeline<_,_,_,_>, Vec<_>)> = timelines.into_iter().filter_map(|mut x| x.take()).collect();

    if !result.is_empty() {
        let (timelines, spawn_metrics): (Vec<streaming_harness::timeline::Timeline<_,_,_,_>>, Vec<_>) = result.into_iter().unzip();
        let ::streaming_harness::timeline::Timeline { timeline, .. } = ::streaming_harness::output::combine_all(timelines);

        let spawn_metrics = spawn_metrics.first().unwrap(); // only worker 0 measures spawn new processes

        println!("{}", ::streaming_harness::format::format_summary_timeline("summary_timeline".to_string(), timeline.clone()));
        for (bootstrap, mv) in spawn_metrics.iter() {
            println!("spawn_metric\t{}\t{}", bootstrap, mv);
        }

        let mut metrics = File::create("metrics").unwrap();
        metrics.write(::streaming_harness::format::format_summary_timeline("summary_timeline".to_string(), timeline.clone()).as_bytes()).unwrap();
        metrics.write(b"\n").unwrap();
        for (bootstrap, mv) in spawn_metrics.iter() {
            metrics.write(format!("spawn_metric\t{}\t{}\n", bootstrap, mv).as_bytes()).unwrap();
        }
    }
}
