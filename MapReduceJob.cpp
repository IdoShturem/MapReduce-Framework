#include "MapReduceJob.h"
#include <thread>
/*
===============================================
Implement:
===============================================
*/

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel):
client(client), 
input_vector(inputVec),
multy_thread_level(multiThreadLevel),
is_done(false), 
is_join(false), 
next_input_index(0),
barrier(multiThreadLevel)
{
   // init 2 bit for state, 31 bit total num of keys to process, 31 bits already proccesed keys
   uint64_t init_stage = static_cast<uint64_t>(MAP_STAGE) << 62;
   uint64_t total_process= static_cast<uint64_t>(input_vector.size()) << 31;
   job_state.store(init_stage | total_process | 0);

   // resize to match number of threads given 
   intermediate_vectors.resize(multy_thread_level);
   
   // init thread vectors
   threads_vector.reserve(multy_thread_level);
   for(int i=0; i < multy_thread_level; ++i){
        threads_vector.push_back(std::thread(&MapReduceJob::thread_func,this,i));
   }
}

MapReduceState MapReduceJob::getState(void) const
{
    MapReduceState state;
    // extract data from job_state
    uint64_t current_state = job_state.load();
    uint64_t stage_val = current_state >> 62; 
    uint64_t total_val = (current_state >> 31) & 0x7FFFFFFF;
    uint64_t processed_val = current_state & 0x7FFFFFFF;
    state.stage = static_cast<MapReduceStage>(stage_val);
    if (total_val == 0) {
        state.percentage = 0.0;
    } else {
        state.percentage = (static_cast<double>(processed_val) / static_cast<double>(total_val)) * 100.0;
    }
    return state;
}

void MapReduceJob::wait(void)
{
    std::lock_guard<std::mutex> lock(wait_mtx);
    if(is_join.load()) return; // already joined
    for(auto& thread : threads_vector){
        if(thread.joinable()){
            thread.join();
        }
    }
    is_join.store(true);
}

OutputVec MapReduceJob::getOutput(void)
{
    // TODO: implement this function
}

bool MapReduceJob::isDone(void) const
{
    // TODO: implement this function
}

MapReduceJob::~MapReduceJob()
{
    // TODO: implement this destructor
}
