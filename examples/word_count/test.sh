#!/bin/bash

export COMPILE_FLAGS=${COMPILE_FLAGS}

export MODE=${MODE:-"sequential"}
export REDUCE_NUM=${REDUCE_NUM:-5}
export WORKER_NUM=${WORKER_NUM:-1}
export WORKER_LIMIT=${WORKER_LIMIT:-1}

go build ${COMPILE_FLAGS} -o wc main.go

if [ ${MODE} == "sequential" ]; then
    ./wc master test $@ \
        --root-dir=/Users/malin/Documents/code/Go/src/github.com/mlmhl/mapreduce/examples/word_count \
        --reduce-num=${REDUCE_NUM} \
        --mode=Sequential \
        -v=3 --logtostderr=true
else
    nohup ./wc master test $@ \
        --root-dir=/Users/malin/Documents/code/Go/src/github.com/mlmhl/mapreduce/examples/word_count \
        --reduce-num=${REDUCE_NUM} \
        --address=tcp:127.0.0.1:12321 \
        --mode=Parallel \
        -v=3 --logtostderr=true > master.log 2>&1 &

    # Wait a moment to make sure Matser started.
    echo "Waiting master started..."
    sleep 2

    for ((i=0;i<${WORKER_NUM};i++))
    do
        nohup ./wc worker worker_${i} test \
        --root-dir=/Users/malin/Documents/code/Go/src/github.com/mlmhl/mapreduce/examples/word_count \
        --map-num=$# \
        --reduce-num=${REDUCE_NUM} \
        --address=tcp:127.0.0.1:1234${i} \
        --master-address=tcp:127.0.0.1:12321 \
        -v=3 --logtostderr=true > worker_${i}.log 2>&1 &
        echo "start worker ${i}"
    done

    tail -f master.log
fi
