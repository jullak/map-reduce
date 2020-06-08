#!/bin/bash
#
#SBATCH --ntasks=8
#SBATCH --partition=RT
#SBATCH --job-name=example

mpic++ main.cpp mapper.hpp map_reduce.hpp shuffler.hpp reducer.hpp -std=c++14 -fpermissive -o mapreduce
time mpiexec ./mapreduce $1
