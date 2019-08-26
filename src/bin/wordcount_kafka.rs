//! Usage example:
//!
//! We spawn two worker processes in cluster mode, each with a single worker thread
//! (each process must have the same number of worker threads):
//!
//! rescaling-examples $ cargo run --bin wordcount -- -n2 -w1 -p0
//! rescaling-examples $ cargo run --bin wordcount -- -n2 -w1 -p1
//!
//! After a few seconds (it can be more but instructions are hardcoded
//! and we need to spawn the 3rd worker process before the configuration update
//! migrating state to the new worker is issued) we can spawn the 3rd worker process:
//!
//! cargo run --bin wordcount -- -n2 -w1 -p2 --join 0 --nn 3
//!                              <----->
//!                                 ^should always remain unchanged
//!
//! The arguments have the following semantic:
//! --join 0 => join the cluster using worker with index 0 as the bootstrap server
//! --nn 3   => the new number of worker in the cluster
//!
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Map, Inspect, Probe};
use timely::dataflow::operators::aggregation::StateMachine;
use dynamic_scaling_mechanism::state_machine::BinnedStateMachine;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::io::{BufReader, BufRead};
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::exchange::Exchange;
use colored::Colorize;
use rescaling_examples::verify;
use std::cell::RefCell;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let mut lines_in = InputHandle::new();

        let mut stateful_probe = ProbeHandle::new();
        let mut correct_probe = ProbeHandle::new();

        let widx = worker.index();

        worker.dataflow::<usize, _, _>(|scope| {

            let mut words_probe = ProbeHandle::new();

            let rr = RefCell::new(0_u64);

            let words_in =
                lines_in
                    .to_stream(scope)
                    .exchange(move |_| { let mut rr = rr.borrow_mut(); *rr+=1; *rr }) // round-robin
                    .flat_map(|text: String|
                        text.split_whitespace()
                            .map(move |word| (word.to_owned(), 1))
                            .collect::<Vec<_>>()
                    ).probe_with(&mut words_probe);

            let control = rescaling_examples::kafka::control_stream(scope, words_probe, widx).broadcast();

            control.inspect(|c| println!("{}", format!("control message is {:?}", c).bold().yellow()));

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
                    }, |_key| 0) // need to send everything to worker 0 as number of peers changes and it would compute the wrong answer (no routing table without megaphone)
                    .inspect(move |x| println!("[W{}] correct seen: {:?}", widx, x))
                    .probe_with(&mut correct_probe);

            verify(&correct.exchange(|_| 0), &stateful_out.exchange(|_| 0));
        });

        // IMPORTANT: allow a worker joining the cluster to do its initialization.
        // If the worker running this code:
        //   - is not joining the cluster, this is a no-op and will return false.
        //   - is joining the cluster, it will perform the bootstrapping protocol to initialize its state
        //     and will return true. We return from the function (worker will run to completion) as it
        //     should not inject any input.
        if worker.bootstrap() { return; }

        if worker.index() == 0 {
            let reader = BufReader::new(File::open("text/sample.txt").unwrap());
            for (round, line) in reader.lines().enumerate() {
                lines_in.send(line.unwrap());
                std::thread::sleep(std::time::Duration::from_millis(500));
                lines_in.advance_to(round + 1);
                worker.step_while(|| stateful_probe.less_than(lines_in.time()));
                worker.step_while(|| correct_probe.less_than(lines_in.time()));
            }
        }
    }).unwrap();
}

