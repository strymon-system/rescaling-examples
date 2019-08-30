
import matplotlib.pyplot as plt

times = []
percentiles = []
spawn_metrics = []

ns_to_sec    = 1000000000
ns_to_millis = 1000000

with open("metrics", "rt") as f:
    for line in f.readlines():
        tokens = line.split("\t")
        if tokens[0] == "summary_timeline":
            times.append(float(tokens[1])/ns_to_sec)
            percentiles.append([float(p)/ns_to_millis for p in tokens[2:-2]])
        if tokens[0] == "spawn_metric":
            bootstrap = float(tokens[1])/ns_to_sec
            move = float(tokens[2])/ns_to_sec
            spawn_metrics.append((bootstrap, move))


plt.plot(times, percentiles, )#, "p99.9", ))#"max"))
for (bootstrap, move) in spawn_metrics:
    plt.axvline(x=bootstrap, linewidth=3, color="y", alpha=.3)
    plt.axvline(x=move, linewidth=3, color="b", alpha=.3)
plt.legend(("p25", "p50", "p75", "p95", "p99", "bootstrap", "move"))
plt.xlabel("Time [s]") 
plt.ylabel("Latency [ms]") 
plt.savefig("report/imgs/latency.pdf")
