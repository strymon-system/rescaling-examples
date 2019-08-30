pub mod kafka;

use timely::dataflow::{Scope, Stream};
use timely::ExchangeData;
use std::collections::{HashMap, VecDeque};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Exchange;
use rand::{Rng, SeedableRng};
use rand::seq::IteratorRandom;
use rand::rngs::StdRng;

pub fn verify<S: Scope, T: ExchangeData + Ord + ::std::fmt::Debug>(correct: &Stream<S, T>, output: &Stream<S, T>) -> Stream<S, ()> {
    let mut in1_pending: HashMap<_, Vec<_>> = Default::default();
    let mut in2_pending: HashMap<_, Vec<_>> = Default::default();
    let mut data_buffer: Vec<T> = Vec::new();
    correct.binary_notify(
        &output,
        Exchange::new(|_| 0),
        Exchange::new(|_| 0),
        "Verify",
        vec![],
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
                // println!("comparing time {:?}\n\tin1 = {:?}\n\tin2 = {:?}", *time.time(), in1_pending, in2_pending);
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

pub struct LinesGenerator {
    distinct_words: Vec<String>,
    words_per_line: usize,
    rng: rand::rngs::ThreadRng,
}

impl LinesGenerator {
    pub fn new(distinct_words: usize, words_per_line: usize, word_length: usize) -> Self {
        let mut rng = rand::thread_rng();
        let distinct_words = (0..distinct_words).map(|_| {
            (0..word_length).map(|_| rng.sample(rand::distributions::Alphanumeric)).collect::<String>()
        }).collect();

        LinesGenerator {
            distinct_words,
            words_per_line,
            rng
        }
    }

    pub fn next(&mut self) -> String {
        (0..self.words_per_line)
            .map(|_| self.distinct_words.iter().choose(&mut self.rng).unwrap().clone())
            .collect::<Vec<String>>()
            .join(" ")
    }

    pub fn word_at(&self, index: usize) -> String {
        self.distinct_words[index].clone()
    }
}

pub enum WordGenerator {
    Uniform(StdRng, usize),
}

impl WordGenerator {

    pub fn new_uniform(index: usize, keys: usize) -> Self {
        let seed: [u8; 32] = [1, 2, 3, 4, 5, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, index as u8];
        WordGenerator::Uniform(SeedableRng::from_seed(seed), keys)
    }

    #[inline(always)]
    pub fn word_rand(&mut self) -> usize {
        let index = match *self {
            WordGenerator::Uniform(ref mut rng, ref keys) => rng.gen_range(0, *keys),
        };
        self.word_at(index)
    }

    #[inline(always)]
    pub fn word_at(&mut self, k: usize) -> usize {
        k
    }
}

pub struct LoadBalancer {
    bins: usize,
    worker2bins: HashMap<usize, VecDeque<usize>>,
}

impl LoadBalancer {
    pub fn new(workers: Vec<usize>, bins: usize) -> Self {
        // initialize the mapping as inside the stateful operators
        let map: Vec<usize> = (0..workers.len()).cycle().take(bins).collect();
        let worker2bins = workers.iter().map(|worker| {
            (*worker, map.iter().enumerate().filter_map(|(i, w)| if w == worker { Some(i) } else { None }).collect())
        }).collect();

        LoadBalancer { bins, worker2bins }
    }
    pub fn add_workers(&mut self, new_workers: Vec<usize>) -> impl Iterator<Item=(usize, usize)> {
        // initial empty assignments
        self.worker2bins.extend(new_workers.iter().map(|w| (*w, VecDeque::new())));

        let mut moves = Vec::new();

        loop {
            let max = self.worker2bins.iter().max_by_key(|(_w, bins)| bins.len()).unwrap();
            let min = self.worker2bins.iter().min_by_key(|(_w, bins)| bins.len()).unwrap();

            let delta =  max.1.len() - min.1.len();
            if delta <= 1 { break } // we are done, work is balanced

            let loaded_w = *max.0;
            let not_loaded_w = *min.0;
            let bin_to_move = self.worker2bins.get_mut(&loaded_w).unwrap().pop_back().unwrap();
            self.worker2bins.get_mut(&not_loaded_w).unwrap().push_back(bin_to_move);
            moves.push((bin_to_move, not_loaded_w));
        }
        moves.into_iter()
    }

    pub fn dump_map(&self) {
        let mut map = self.worker2bins.iter().collect::<Vec<_>>();
        map.sort();
        println!("map: {:?}", map);
    }

    #[allow(dead_code)]
    fn verify(&self) {
        // self.dump_map();
        // properly balanced
        let max = self.worker2bins.values().map(|bins| bins.len()).max().unwrap();
        let min = self.worker2bins.values().map(|bins| bins.len()).min().unwrap();
        assert!(max - min <= 1);

        // every bin is assigned
        let assigned_bins: usize = self.worker2bins.values().map(|bins| bins.len()).sum();
        assert_eq!(assigned_bins, self.bins);
    }
}

mod test {

    #[test]
    fn load_balancer_init() {
        let workers = (0..6_usize).collect::<Vec<_>>();
        let mut lb = crate::LoadBalancer::new(workers, 50);
        lb.dump_map();

        let new_workers = (6..60_usize).collect::<Vec<_>>();
        lb.add_workers(new_workers);

        lb.dump_map();
    }
}
