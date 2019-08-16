use timely::dataflow::{Scope, Stream};
use timely::ExchangeData;
use std::collections::HashMap;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Exchange;

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
