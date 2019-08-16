
## Dynamic Rescaling of Timely Dataflow

We demonstrate how one could write timely dataflow programs that support dynamic rescaling at
runtime: you can add worker to the computation and leverage [Megaphone](https://github.com/strymon-system/megaphone)
to perform state migrations.

The examples depend on the `rescaling-p2p` and `rescaling` branches of `timely` and `megaphone` respectively (see `Cargo.toml` for more details).

## Sample usage - WordCount

We spawn two worker processes in cluster mode, each with a single worker thread
(each process must have the same number of worker threads):

`rescaling-examples $ cargo run --bin wordcount -- -n2 -w1 -p0`  
`rescaling-examples $ cargo run --bin wordcount -- -n2 -w1 -p1`

After a few seconds, we can spawn the 3rd worker process (it could be more, but instructions are hardcoded
and we need to spawn the 3rd worker process before the configuration update
migrating state to it is issued) :

`cargo run --bin wordcount -- -n2 -w1 -p2 --join 0 --nn 3`

The arguments have the following semantic:
* `-n2` : the initial number of workers in the cluster (should always be 2 event in following rescaling operations) 
* `-w1` : a single worker thread (must be the same as the other worker processes)
* `-p2` : the process index (for now it is expected to be the `+1` to the max process index currently in the cluster)
* `--join 0` : join the cluster using worker with index 0 as the bootstrap server
* `--nn 3`   : the new number of processes in the cluster

You should see some debug prints, and after the configuration change command
migrating state to the new worker is issued it will also start producing output.

Correctness of the count is verified in the code (stole the `verify` code from Megaphone, thanks),
so there's no need to stare at the output.
