#!/usr/bin/env bash

export NRUNS=10
export JSON=$( gh run list -R g-adopt/g-adopt -w test.yml -s success -L "${NRUNS}" --json databaseId,number )
declare -a RUNIDS=( $( jq --jsonargs '.[].databaseId' <<<"${JSON}" ) )
declare -a RUNNOS=( $( jq --jsonargs '.[].number' <<<"${JSON}") )

for (( i=0; i<${NRUNS}; i++ )); do
    gh run download -R g-adopt/g-adopt -D "${RUNNOS[$i]}" -n run-log "${RUNIDS[$i]}"
done

memprof_plotter "${RUNNOS[@]}"
