//
// Created by Iulia Akzhigitova on 2019-10-12.
//

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <mpi.h>
#include <string>
#include <vector>


template <typename InKey, typename InValue>
class Shuffler
{
private:
    template <typename T=InKey, typename B=InValue>
    struct stream_iterator
    {
        stream_iterator(const std::string& file_name, size_t reduce_id, size_t reduce_count) {
            this->reduce_id = reduce_id;
            this->reduce_count = reduce_count;
            in.open(file_name, std::ios_base::in);

            ++(*this);
        }
        stream_iterator(stream_iterator&& si) {
            in = std::ifstream(std::move(si.in));
            current_key = std::move(si.current_key);
            current_value = std::move(si.current_value);
            reduce_id = si.reduce_id;
            reduce_count = si.reduce_count;
        }

        stream_iterator& operator=(stream_iterator&& si) {
            if (&si == this) return *this;

            in = std::ifstream(std::move(si.in));
            current_key = std::move(si.current_key);
            current_value = std::move(si.current_value);
            reduce_id = si.reduce_id;
            reduce_count = si.reduce_count;

            return *this;
        }
        ~stream_iterator() {
            in.close();
        }

        stream_iterator& operator++() {
            in >> current_key;
            while (in && hash_key(current_key) % reduce_count != reduce_id) {
                in >> current_value;
                in >> current_key;
            }
            in >> current_value;

            return *this;
        }

        bool operator < (const stream_iterator& r) const {
            return r.current_key < this->current_key;
        }

        bool is_end() const {
            return !static_cast<bool>(in);
        }

        std::ifstream in;
        size_t reduce_id;
        size_t reduce_count;

        T current_key;
        B current_value;
        std::hash<T> hash_key;
    };

public:
    explicit Shuffler(const std::vector<std::string>& file_names, size_t reduce_id, size_t reduce_count)
        : reduce_id_(reduce_id)
        , reduce_count_(reduce_count)
        , file_names_(std::move(file_names))
    {
        for (size_t i = 0; i < file_names_.size(); ++i) {
            inputs_.push_back(stream_iterator<InKey, InValue>(file_names_[i], reduce_id_, reduce_count_));
        }
        out_for_reducer_.open("reducer_" + std::to_string(reduce_id_));
    }
    ~Shuffler() {
        out_for_reducer_.close();

        for (auto& name : file_names_) {
            std::remove(name.data());
        }
    }

    Shuffler(const Shuffler& s) = delete;
    Shuffler& operator= (const Shuffler& s) = delete;


    //external sort
    void process() {
        size_t opened_fd = inputs_.size();

        InKey unions_key, curr_key;
        std::vector<InValue> value_list;

        std::make_heap(inputs_.begin(), inputs_.end());

        std::pop_heap(inputs_.begin(), inputs_.end());
        unions_key = inputs_.back().current_key;
        while (true) {
            do {
                value_list.push_back(inputs_.back().current_value);

                ++inputs_.back();
                if (inputs_.back().is_end()) {
                    --opened_fd;
                    inputs_.pop_back();
                    std::make_heap(inputs_.begin(), inputs_.end());
                }
                else {
                    std::push_heap(inputs_.begin(), inputs_.end());
                }

                if (!opened_fd) break;

                std::pop_heap(inputs_.begin(), inputs_.end());
                curr_key = inputs_.back().current_key;
            } while (curr_key == unions_key);

            write_to_file(unions_key, value_list);
            value_list.clear();

            unions_key = curr_key;

            if (!opened_fd) break;
        }
    }

private:
    //int rank_, world_size_;
    size_t reduce_id_, reduce_count_;
    std::vector<std::string> file_names_;

    std::vector<stream_iterator<InKey, InValue>> inputs_;
    std::ofstream out_for_reducer_;

    void write_to_file(InKey key, const std::vector<InValue>& values) {
        out_for_reducer_ << key << '\t';
        for (const auto& value : values) {
            out_for_reducer_ << value << " ";
        }
        out_for_reducer_ << '\n';
    }

};
