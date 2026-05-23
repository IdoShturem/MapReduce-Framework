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

class IdentityClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override
    {
        context.addIntermediate(std::dynamic_pointer_cast<IntElement>(key),
                                std::dynamic_pointer_cast<IntElement>(value));
    }
    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override
    {
        for (auto &p : pairs)
            context.addOutput(std::dynamic_pointer_cast<IntElement>(p.first),
                              std::dynamic_pointer_cast<IntElement>(p.second));
    }
};

int main()
{
    IdentityClient client;
    InputVec inputVec; // empty
    MapReduceJob job(client, inputVec, 4);
    auto output = job.getOutput();
    std::cout << "empty output: " << (output.empty() ? "true" : "false") << std::endl;
    return 0;
}
