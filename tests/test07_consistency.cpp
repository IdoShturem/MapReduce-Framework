#include "MapReduceJob.h"
#include <iostream>
#include <vector>

static const int UNIQUE_KEYS = 10;
static const int INPUT_SIZE  = 1000;

class IntElement : public K1, public K2, public K3, public V1, public V2, public V3
{
public:
    int num;
    IntElement(int n) : num(n) {}
    bool operator<(const K1 &other) const override { return num < dynamic_cast<const IntElement&>(other).num; }
    bool operator<(const K2 &other) const override { return num < dynamic_cast<const IntElement&>(other).num; }
    bool operator<(const K3 &other) const override { return num < dynamic_cast<const IntElement&>(other).num; }
};

class ModuloClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override
    {
        int k = std::dynamic_pointer_cast<IntElement>(key)->num % UNIQUE_KEYS;
        context.addIntermediate(std::make_shared<IntElement>(k), std::make_shared<IntElement>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override
    {
        int k = std::dynamic_pointer_cast<IntElement>(pairs[0].first)->num;
        int count = static_cast<int>(pairs.size());
        context.addOutput(std::make_shared<IntElement>(k), std::make_shared<IntElement>(count));
    }
};

static InputVec make_input()
{
    InputVec v;
    for (int i = 0; i < INPUT_SIZE; ++i)
        v.push_back({std::make_shared<IntElement>(i), std::make_shared<IntElement>(i)});
    return v;
}

static std::vector<std::pair<int,int>> collect(OutputVec &out)
{
    std::vector<std::pair<int,int>> result;
    for (auto &p : out)
        result.push_back({std::dynamic_pointer_cast<IntElement>(p.first)->num,
                          std::dynamic_pointer_cast<IntElement>(p.second)->num});
    return result;
}

int main()
{
    ModuloClient client;

    InputVec in1 = make_input();
    MapReduceJob job1(client, in1, 1);
    OutputVec out1 = job1.getOutput();
    auto r1 = collect(out1);

    InputVec in2 = make_input();
    MapReduceJob job2(client, in2, 4);
    OutputVec out2 = job2.getOutput();
    auto r2 = collect(out2);

    bool consistent = (r1 == r2);
    std::cout << "consistent: " << (consistent ? "true" : "false") << std::endl;
    return consistent ? 0 : 1;
}
