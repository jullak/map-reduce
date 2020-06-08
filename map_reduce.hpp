//
// Created by Iulia Akzhigitova on 2019-10-14.
//

#pragma once

#include "mapper.hpp"
#include "reducer.hpp"
#include "shuffler.hpp"

#include <mpi.h>

template <typename M, typename R>
class MapReduce
{
public:
    explicit MapReduce(const std::string& file, size_t reducer_count=default_reducer_count)
        : reducer_count_(reducer_count)
    {
        MPI_Init(NULL, NULL);

        MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
        MPI_Comm_size(MPI_COMM_WORLD, &world_size_);

        mapper_ = new M();
        reducer_ = new R();

        mapper_->set_source_files(file);
    }
    ~MapReduce() {
        delete mapper_;
        delete reducer_;

        MPI_Finalize();
    }

    MapReduce(const MapReduce& mr) = delete;
    MapReduce& operator = (const MapReduce& mr) = delete;

    void execute() {
        mapper_->set_rank();
        mapper_->set_world_size();
        mapper_->run();

        std::vector <std::string> file_after_map;

        std::vector<int> total_iter = mapper_->get_total_iteration_count();
        for (int i = 1; i < world_size_; ++i) {
            for (int j = 0; j < total_iter[i]; ++j) {
                file_after_map.push_back("tmp_map_" + std::to_string(i) + "iter" + std::to_string(j));
            }
        }

        for (size_t i = rank_; i < reducer_count_; i += world_size_) {
            Shuffler<typename M::Key, typename M::Value> shuffler(file_after_map, i, reducer_count_);
            shuffler.process();
        }

        for (size_t i = rank_; i < reducer_count_; i += world_size_) {
            reducer_->set_id(i);
            reducer_->run();
        }
    }

private:
    int rank_, world_size_;
    size_t reducer_count_ = 0;

    M * mapper_;
    R * reducer_;
};