#include <iostream>
#include "map_reduce.hpp"

class MyMapper : public Mapper<std::string, int>
{
public:
    std::vector<std::pair<std::string, int>> map(char * val, size_t size)
    {
        std::vector<std::pair<std::string, int>> res;
        std::string str_m = "";

        for (size_t i = 0; i < size; ++i)
        {
            while (i < size && !isspace(val[i])) {
                str_m += val[i];
                ++i;
            }

            if (!str_m.empty()) res.push_back({str_m, 1});
            str_m = "";
        }

        return res;
    }
};


class MyReducer : public Reducer<std::string, int, std::string, int>
{
public:
    ReduceContainer reduce(std::string& key, std::vector<int>& values)
    {
        return {key, values.size()};
    }
};



int main(int argc, char * argv[]) {

    if (argc > 1) {
        MapReduce<MyMapper, MyReducer> mr("data", std::stoi(argv[1]));
        mr.execute();
    } else {
        MapReduce<MyMapper, MyReducer> mr("data");
        mr.execute();
    }

    return 0;
}
