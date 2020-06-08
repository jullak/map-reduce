//
// Created by Iulia Akzhigitova on 2019-10-14.
//

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <mpi.h>
#include <string>
#include <sstream>
#include <vector>


template <typename InKey, typename InValue, typename OutKey, typename OutValue>
class Reducer
{
public:
    using ReduceContainer = std::pair<OutKey, OutValue>;

    Reducer() {
        MPI_File_open(MPI_COMM_WORLD, "result", MPI_MODE_WRONLY | MPI_MODE_CREATE, MPI_INFO_NULL, &output_file_);
    }
    Reducer(const Reducer& reducer) = delete;
    Reducer& operator= (const Reducer& reducer) = delete;

    ~Reducer() {
        MPI_File_close(&output_file_);
    }

    void set_id(size_t id) {
        id_ = id;
        input_file_.open("reducer_" + std::to_string(id_));
    }

    virtual ReduceContainer reduce(InKey& key, std::vector<InValue>& values) = 0;

    void run() {
        InValue val;
        std::vector<InValue> values;

        while (input_file_) {
            std::stringstream ss;
            std::string key_values;
            InKey key;

            std::getline(input_file_, key_values);
            ss << key_values;
            std::getline(ss, key, '\t');
            ss.ignore();

            while (ss) {
                ss >> val;
                values.push_back(val);
            }

            ReduceContainer reduced_data = reduce(key, values);
            write_to_file(reduced_data);

            values.clear();
        }

        clear();
    }

private:
    //int rank_, world_size_;
    size_t id_;

    std::ifstream input_file_;
    MPI_Status status_;
    MPI_File output_file_;

    void write_to_file(const ReduceContainer& data) {
        std::stringstream result_stream;
        result_stream << data.first << "\t" << data.second << '\n';

        std::string result;
        std::getline(result_stream, result);
        result += '\n';

        MPI_File_write_shared(
                output_file_,
                (char *)result.data(),
                result.size(),
                MPI_CHAR,
                &status_);
    }

    void clear() {
        input_file_.close();
        std::string name = "reducer_" + std::to_string(id_);
        std::remove(name.data());
    }
};
