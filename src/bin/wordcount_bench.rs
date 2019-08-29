use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Inspect, Probe};
use dynamic_scaling_mechanism::state_machine::BinnedStateMachine;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::exchange::Exchange;
use colored::Colorize;
use rescaling_examples::{LinesGenerator, LoadBalancer};
use std::cell::RefCell;
use dynamic_scaling_mechanism::{ControlInst, BinId, Control, BIN_SHIFT};
use std::process::Command;
use std::collections::VecDeque;
use std::time::Instant;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;
use std::rc::Rc;
use std::fs::File;

const WORKER_BOOTSTRAP_MARGIN: u128 = 500 * 1_000_000; // 500 millis (as nanoseconds)

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

        let widx = worker.index();

        let latencies1 = Rc::new(RefCell::new(Vec::<u128>::new()));
        let latencies2 = Rc::clone(&latencies1);

        let epoch_timer1 = Rc::new(Instant::now());
        let epoch_timer2 = Rc::clone(&epoch_timer1);
        let epoch_timer3 = Rc::clone(&epoch_timer1);

        worker.dataflow::<u128,_,_>(|scope| {
            let rr = RefCell::new(0_u64);

            let words_in =
                lines_in
                    .to_stream(scope)
                    .exchange(move |_| { let mut rr = rr.borrow_mut(); *rr+=1; *rr }) // round-robin
                    .flat_map(move |text: String| {
                        let enter_time = epoch_timer1.elapsed().as_nanos();
                        text.split_whitespace()
                            .map(move |word| (word.to_owned(), (1, enter_time)))
                            .collect::<Vec<_>>()
                    });

            let control = control_in.to_stream(scope).broadcast();

            control.inspect(move |c| println!("[W{}] {}", widx, format!("control message is {:?}", c).bold().yellow()));

            words_in
                .stateful_state_machine(|key: &String, val, agg: &mut (u64, u128)| {
                    let (count, timestamp) = val;
                    agg.0 += count;
                    agg.1 = timestamp;
                    (false, Some((key.clone(), *agg)))
                }, |key| calculate_hash(key), &control)
                .probe_with(&mut stateful_probe)
                .sink(Pipeline, "delta_timestamps", move |input| {
                    let mut _latencies_stash = latencies1.borrow_mut();
                    let end_time = epoch_timer2.elapsed().as_nanos();
                    while let Some((time, data)) = input.next() {
                        for (_word, (_count, start_time)) in data.iter() {
                            println!("start={:?} end={:?}", start_time, end_time);
                            let latency = end_time - start_time;
                            println!("{:?}:\t{:?}", *time.time(), latency);
                        }
                    }
                });
        });

        if worker.bootstrap() {
            while worker.step_or_park(None) {}
            // TODO save_latencies()
        }

        let mut lines_gen = LinesGenerator::new(1000, 10);

        // epochs ~= ns
        let mut spawn_at_epochs = VecDeque::new();
        spawn_at_epochs.push_back(10*1_000_000_000); // 10 seconds

        let n = std::env::var("N").expect("missing N env var -- number of processes").parse::<usize>().unwrap();
        let w = std::env::var("W").expect("missing W env var -- number of workers").parse::<usize>().unwrap();
        let mut p = n;
        let mut nn = n+1;
        let mut join = 0;

        let mut spawn_info = None;

        let mut sequence = 0; // control cmd sequence number

        let mut load_balancer = LoadBalancer::new((0..worker.peers()).collect::<Vec<usize>>(), 1<<BIN_SHIFT);

        loop {
            let epoch = epoch_timer3.elapsed().as_nanos();

            if widx == 0 {
                if let Some(spawn_at_epoch) = spawn_at_epochs.front() {
                    if epoch >= *spawn_at_epoch {
                        spawn_at_epochs.pop_front();

                        let old_peers = worker.peers();

                        let file = File::create("foo.txt").unwrap();

                        Command::new("cargo")
                            .stdout(file)
                            .arg("run")
                            .arg("--bin")
                            .arg("wordcount_bench")
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
                            .map(|cmd| Control::new(sequence, w, cmd))
                            .for_each(|ctrl| control_in.send(ctrl));

                        sequence += 1;

                        assert!(spawn_info.is_none());
                        spawn_info = Some((p, epoch));

                        p += 1;
                        nn += 1;
                        join += 1;
                        join %= worker.peers();
                    }
                }

                let mut bin_moved = false;
                if let Some((new_process, bootstrap_epoch)) = spawn_info {
                    if epoch > bootstrap_epoch + WORKER_BOOTSTRAP_MARGIN {
                        // move about 1/peers of the bins to the new `w` workers
                        let new_workers = (new_process*w..new_process*w+w).collect::<Vec<_>>();
                        let moves = load_balancer.add_workers(new_workers).map(|(bin, to)| ControlInst::Move(BinId::new(bin), to)).collect::<Vec<_>>();
                        let count = moves.len();
                        moves
                            .into_iter()
                            .map(|mv| Control::new(sequence, count, mv))
                            .for_each(|ctrl| control_in.send(ctrl));

                        sequence += 1;
                        bin_moved = true;
                    }
                }
                if bin_moved { spawn_info = None; }
            }

            // TODO(lorenzo) maybe try streaming harness lib
            if epoch < 20*1_000_000_000 { // 20 seconds
                lines_in.send(lines_gen.next());
            } else {
                break
            }

            lines_in.advance_to(epoch);
            control_in.advance_to(epoch);

            worker.step();
        }

        // close input streams
        lines_in.close();
        control_in.close();

        worker.step_while(|| !stateful_probe.done());

        // TODO save_latencies()
    }).unwrap();
}
