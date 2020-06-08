//
// Created by Iulia Akzhigitova on 2019-10-12.
//

#include <algorithm>
#include <cstring>
#include <functional>
#include <fstream>
#include <mpi.h>
#include <string>
#include <vector>

#include "constant.hpp"

template <typename OutKey, typename OutValue>
class Mapper
{
public:
    using MapContainer = std::vector<std::pair<OutKey, OutValue>>;
    using Key = OutKey;
    using Value = OutValue;

    Mapper() {}
    Mapper(const Mapper& mapper) = delete;
    Mapper& operator = (const Mapper& mapper) = delete;

    void set_rank() {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
    }
    void set_world_size() {
        MPI_Comm_size(MPI_COMM_WORLD, &world_size_);
        total_iteration_.resize(world_size_);
    }

    void set_source_files(const std::string& source) {
        current_source_ = source;
    }

    void run() {
        if (rank_ == 0) {
            cut_and_send();
            MPI_Barrier(MPI_COMM_WORLD);

            collect_total_iteration();
        } else {
            map_it();
            MPI_Barrier(MPI_COMM_WORLD);

            send_iteration();
            collect_total_iteration();
        }
    }

    std::vector<std::string> get_result_file_names() const {
        return result_file_names_;
    }

    std::vector<int> get_total_iteration_count() const {
        return total_iteration_;
    }

    virtual MapContainer map(char * val, size_t size) = 0;

private:
    static const size_t BLOCK_SIZE = 1 << 16;
    int rank_, world_size_;
    std::string current_source_;
    std::ifstream input_file_;

    size_t worker_iteration_ = 0;
    size_t offset_ = 0;

    MPI_Status status_;
    MPI_Request request_;

    std::vector<std::string> result_file_names_;
    std::vector<int> total_iteration_;

    void cut_and_send() {
        size_t file_size = get_file_size(current_source_);
        size_t process_num = 1;

        while (offset_ + BLOCK_SIZE < file_size) {
            MPI_Isend(&offset_, 1, MPI_INT, process_num, message_tag::data, MPI_COMM_WORLD, &request_);

            offset_ += BLOCK_SIZE;
            process_num = (process_num + 1) % world_size_;
            process_num = process_num == 0 ? 1 : process_num;
        }
        MPI_Isend(&offset_, 1, MPI_INT, process_num, message_tag::data, MPI_COMM_WORLD, &request_);

        for (size_t proc = process_num; proc < world_size_; ++proc) {
            MPI_Isend(nullptr, 0, MPI_INT, proc, message_tag::no_data, MPI_COMM_WORLD, &request_);
        }

        for (size_t proc = 1; proc < process_num; ++proc) {
            MPI_Isend(nullptr, 0, MPI_INT, proc, message_tag::no_data, MPI_COMM_WORLD, &request_);
        }
    }

    void map_it() {
        size_t file_size = get_file_size(current_source_);
        size_t block_size = BLOCK_SIZE;

        MPI_Recv(&offset_, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status_);

        while (status_.MPI_TAG != message_tag::no_data) {
            offset_  = offset_ == 0 ? 0 : offset_ - 1;
            input_file_.open(current_source_, std::ios_base::in);
            input_file_.seekg(offset_, input_file_.beg);

            size_t begin_diff = (offset_ == 0 ? 0 : word_seek(input_file_));
            size_t end_diff = 0;

            if (offset_ + BLOCK_SIZE >= file_size) {
                block_size = file_size - offset_ - 1;
            } else {
                input_file_.seekg(offset_ + block_size + (offset_ == 0 ? 0 : 1), input_file_.beg);
                end_diff = word_seek(input_file_);
            }

            input_file_.seekg(offset_ + begin_diff, input_file_.beg);

            char * data = new char[block_size + end_diff - begin_diff + (offset_ == 0 ? 0 : 1)];
            memset(data, 0, block_size + end_diff - begin_diff + (offset_ == 0 ? 0 : 1));
            input_file_.read(data, block_size + end_diff - begin_diff + (offset_ == 0 ? 0 : 1));

            MapContainer mapped_data = map(data, block_size + end_diff - begin_diff + (offset_ == 0 ? 0 : 1));
            std::sort(mapped_data.begin(), mapped_data.end());
            write_mapped_data(mapped_data);

            ++worker_iteration_;

            input_file_.close();
            delete[] data;

            MPI_Recv(&offset_, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status_);
        }
    }

    void send_iteration() {
        for (size_t proc = 0; proc < world_size_; ++proc) {
            if (proc != rank_) {
                MPI_Isend(&worker_iteration_, 1, MPI_INT, proc, message_tag::data, MPI_COMM_WORLD, &request_);
            }
        }
    }

    void collect_total_iteration() {
        for (size_t proc = 1; proc < world_size_; ++proc) {
            if (proc != rank_) {
                MPI_Recv(&total_iteration_[proc], 1, MPI_INT, proc, MPI_ANY_TAG, MPI_COMM_WORLD, &status_);
            } else {
                total_iteration_[proc] = worker_iteration_;
            }
        }
    }

    void write_mapped_data(MapContainer& data) {
        std::string name = "tmp_map_" + std::to_string(rank_) + "iter" + std::to_string(worker_iteration_);
        result_file_names_.push_back(name);

        std::ofstream output_file_(name);
        for (auto& item : data) {
            output_file_ << item.first << '\t' << item.second << '\n';
        }

        output_file_.close();
    }

    size_t get_file_size(const std::string& name) const {
        std::ifstream file;
        file.open(name);

        file.seekg(0, file.end);
        size_t size = file.tellg();
        file.close();

        return size;
    }

    size_t word_seek(std::ifstream& file) {
        size_t diff = 0;
        char c;
        while (file.get(c) && !isspace(c)) {
            ++diff;
        }

        return diff;
    }
};
