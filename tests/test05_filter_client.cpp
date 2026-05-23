#include "MapReduceJob.h"
#include <iostream>

class IntElement : public K1, public K2, public K3, public V1, public V2, public V3
{
public:
    int num;
    IntElement(int n) : num(n) {}
    bool operator<(const K1 &other) const override { return num < dynamic_cast<const IntElement&>(other).num; }
    bool operator<(const K2 &other) const override { return num < dynamic_cast<const IntElement&>(other).num; }
    bool operator<(const K3 &other) const override { return num < dynamic_cast<const IntElement&>(other).num; }
};

// Only emits intermediate pairs for even-valued keys; skips odd keys entirely.
class FilterClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override
    {
        int k = std::dynamic_pointer_cast<IntElement>(key)->num;
        if (k % 2 == 0)
            context.addIntermediate(std::make_shared<IntElement>(k), std::make_shared<IntElement>(1));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override
    {
        int k = std::dynamic_pointer_cast<IntElement>(pairs[0].first)->num;
        int count = static_cast<int>(pairs.size());
        context.addOutput(std::make_shared<IntElement>(k), std::make_shared<IntElement>(count));
    }
};

int main()
{
    FilterClient client;
    InputVec inputVec;
    for (int i = 0; i < 10; ++i)
        inputVec.push_back({std::make_shared<IntElement>(i), std::make_shared<IntElement>(i)});

    MapReduceJob job(client, inputVec, 4);
    auto output = job.getOutput();
    for (auto &p : output)
    {
        int k = std::dynamic_pointer_cast<IntElement>(p.first)->num;
        int v = std::dynamic_pointer_cast<IntElement>(p.second)->num;
        std::cout << "(" << k << ", " << v << ")" << std::endl;
    }
    return 0;
}
