use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Inspect, Probe};
use timely::dataflow::operators::aggregation::StateMachine;
use dynamic_scaling_mechanism::state_machine::BinnedStateMachine;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::exchange::Exchange;
use colored::Colorize;
use rescaling_examples::{verify, LinesGenerator, LoadBalancer};
use std::cell::RefCell;
use dynamic_scaling_mechanism::{ControlInst, BinId, Control, BIN_SHIFT};
use std::process::Command;
use std::collections::VecDeque;
use std::time::Instant;

const WORKER_BOOTSTRAP_MARGIN: u128 = 500 * 1_000_000; // 500 millis (as nanoseconds)

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let should_verify = false;

        let mut lines_in = InputHandle::new();
        let mut control_in = InputHandle::new();

        let mut stateful_probe = ProbeHandle::new();

        let widx = worker.index();

        worker.dataflow(|scope| {

            let rr = RefCell::new(0_u64);

            let words_in =
                lines_in
                    .to_stream(scope)
                    .exchange(move |_| { let mut rr = rr.borrow_mut(); *rr+=1; *rr }) // round-robin
                    .flat_map(|text: String|
                        text.split_whitespace()
                            .map(move |word| (word.to_owned(), 1))
                            .collect::<Vec<_>>()
                    );

            let control = control_in.to_stream(scope).broadcast();

            control.inspect(move |c| println!("[W{}] {}", widx, format!("control message is {:?}", c).bold().yellow()));

            let stateful_out =
                words_in
                    .stateful_state_machine(|key: &String, val, agg: &mut u64| {
                        *agg += val;
                        (false, Some((key.clone(), *agg)))
                    }, |key| calculate_hash(key), &control)
                    .probe_with(&mut stateful_probe);

            if should_verify {
                let correct =
                    words_in
                        .state_machine(|key: &String, val, agg: &mut u64| {
                            *agg += val;
                            (false, Some((key.clone(), *agg)))
                        }, |_key| 0); // need to send everything to worker 0 as number of peers changes and it would compute the wrong answer (no routing table without megaphone)

                verify(&correct, &stateful_out);
            }
        });

        if worker.bootstrap() { return; }

        let mut lines_gen = LinesGenerator::new(1000, 10);

        let epoch_timer = Instant::now();

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
            let epoch = epoch_timer.elapsed().as_nanos();

            if widx == 0 {
                if let Some(spawn_at_epoch) = spawn_at_epochs.front() {
                    if epoch >= *spawn_at_epoch {
                        spawn_at_epochs.pop_front();

                        let old_peers = worker.peers();

                        Command::new("cargo")
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
    }).unwrap();
}
