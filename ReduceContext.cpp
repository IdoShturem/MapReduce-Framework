#include "ReduceContext.h"
#include "MapReduceJob.h"
#include <mutex>


ReduceContext::ReduceContext(MapReduceJob* job):job(job){}

void ReduceContext::addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value)
{
    // lock the shared resorce
    std::lock_guard<std::mutex> lock(job -> output_mtx);
    // push it to output vec
    job -> output_vector.push_back({key, value});
}