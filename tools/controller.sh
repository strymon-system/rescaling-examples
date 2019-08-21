#!/usr/bin/env bash

topic="megaphone-control"

if [[ -z "$KAFKA" ]]; then
    echo "export KAFKA environment variable with the path to your Kafka installation"
    exit 1
fi

if [[ $# -ne 3 ]]; then
    echo "USAGE:"
    echo "    controller.sh BINARY_NAME INITIAL_NUMBER_OF_PROCESSES WORKER_THREADS_PER_PROCESS"
    echo "EXAMPLE:"
    echo "    controller.sh wordcount_kafka 2 1"
    exit 1
fi

bin="$1"
n="$2"
w="$3"
p=${n} # process indices in increasing sequence

function help() {
    echo "---------"
    echo "Commands:"
    echo "  move BIN_ID TARGET_WORKER -- move ownership of bin BIN_ID to worker TARGET_WORKER"
    echo "  spawn [BOOTSTRAP_SERVER]  -- spawn a new worker and add it to the cluster,"
    echo "                               BOOTSTRAP_SERVER is optional and defaults to worker 0"
    echo "---------"
}

help;

GREEN='\033[1;32m'
NC='\033[0m' # No Color

while true; do

    echo -n -e "${GREEN}$ ${NC}"

    read cmd;

    case ${cmd} in

        move*)
            echo "${cmd}" | $KAFKA/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${topic} || exit 1
            echo " Command sent";;

        spawn*)
            tokens=(${cmd})
            bootstrap_server=${tokens[1]:-0}
            echo "Spawning worker process ${p} using bootstrap_server ${bootstrap_server}:"
            spawn_cmd="cargo run --bin ${bin} -- -n ${n} -w ${w} -p ${p} --join ${bootstrap_server} --nn $((${p}+1))"
            echo "   ${spawn_cmd}"
            if ! [[ -z "${TMUX}" ]]; then
                CUR=`tmux display-message -p '#S:#W.#P'`
                SESS=`tmux display-message -p '#S'`
                SESS=`tmux display-message -p '#S'`
                WIN="worker-${p}"
                tmux new-window -d -n ${WIN}
                tmux send-keys -t ${SESS}:${WIN}.0 "${spawn_cmd}" Enter || exit 1
            else
                `${spawn_cmd} > ~/worker-${p} 2>&1 &`
            fi

            # Important: send "none" command so that the new worker will receive the updated megaphone routing table
            echo "Sending \"none\" control command"
            echo "none" | $KAFKA/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${topic} || exit 1
            echo " Command sent"

            p=$((${p}+1))
            ;;

        *)
            echo "Unrecognized command"
            help
            ;;
    esac
done
