#include "MapReduceJob.h"
#include <thread>
#include <algorithm>

/*
===============================================
Implement:
===============================================
*/

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel):
input_vector(inputVec),
client(client), 
multy_thread_level(multiThreadLevel),
next_input_index(0),
completed_threads(0),
barrier(multiThreadLevel),
is_done(false), 
is_join(false),
shuffle_done(false)
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

void MapReduceJob::map_phase(int thread_id)
{
    MapContext map_context(&intermediate_vectors[thread_id]);
    
    while (true)
    {
        size_t current_index = next_input_index.fetch_add(1);
        
        if (current_index >= input_vector.size())
        {
            break; 
        }
        
        const InputPair& pair = input_vector[current_index];
        client.map(pair.first, pair.second, map_context);
        
        job_state.fetch_add(1);
    }
}

void MapReduceJob::shuffle_phase()
{
    uint64_t total_intermediate_pairs = 0;
        for (int i = 0; i < multy_thread_level; ++i)
        {
            total_intermediate_pairs += intermediate_vectors[i].size();
        }

        uint64_t shuffle_stage_val = static_cast<uint64_t>(SHUFFLE_STAGE) << 62;
        uint64_t new_total_val = total_intermediate_pairs << 31;
        job_state.store(shuffle_stage_val | new_total_val | 0);

        while (true)
        {
            std::shared_ptr<K2> max_key = nullptr;
            for (int i = 0; i < multy_thread_level; ++i)
            {
                if (!intermediate_vectors[i].empty())
                {
                    auto current_key = intermediate_vectors[i].back().first;
                    if (max_key == nullptr || *max_key < *current_key)
                    {
                        max_key = current_key;
                    }
                }
            }

            if (max_key == nullptr)
            {
                break;
            }

            IntermediateVec current_key_vec;
            
            for (int i = 0; i < multy_thread_level; ++i)
            {
                while (!intermediate_vectors[i].empty() && 
                       !(*(intermediate_vectors[i].back().first) < *max_key) && 
                       !(*max_key < *(intermediate_vectors[i].back().first))) 
                {
                    current_key_vec.push_back(intermediate_vectors[i].back());
                    intermediate_vectors[i].pop_back();
                    
                    job_state.fetch_add(1); 
                }
            }

            {
                std::lock_guard<std::mutex> lock(queue_mtx);
                shuffle_queue.push(current_key_vec);
            }
            
        }

        uint64_t reduce_stage_val = static_cast<uint64_t>(REDUCE_STAGE) << 62;
        job_state.store(reduce_stage_val | new_total_val | 0);


        shuffle_done.store(true);
        queue_cnd.notify_all();
}

void MapReduceJob::thread_func(int thread_id)
{
    // stage 1 - map phase  
    map_phase(thread_id);

    // stage 2 - sort phase
    std::sort(intermediate_vectors[thread_id].begin(), 
              intermediate_vectors[thread_id].end(),
              [](const IntermediatePair& a, const IntermediatePair& b) {
                  return *(a.first) < *(b.first);
              });

    //barrier
    // wait for all threads to finish the map phase and sort their intermediate vectors before to shuffle
    barrier.arrive_and_wait();

    // stage 3 - shuffle phase
    if (thread_id == 0)
    {
        shuffle_phase();
    }

    // stage 4 - reduce phase
    ReduceContext reduce_context(this);

    while (true)
    {
        IntermediateVec current_vec;
        // scope lock
        {
            std::unique_lock<std::mutex> lock(queue_mtx);
            
            queue_cnd.wait(lock, [this]{ return !shuffle_queue.empty() || shuffle_done.load(); });
            
            if (shuffle_queue.empty() && shuffle_done.load())
            {
                break;
            }
            
            current_vec = shuffle_queue.front();
            shuffle_queue.pop();
        } // mutex released here auto 
        
        
        client.reduce(current_vec, reduce_context);
        
      
        job_state.fetch_add(current_vec.size());
    }
    
    if (completed_threads.fetch_add(1) == multy_thread_level - 1)
    {
        is_done.store(true);
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
    // wait for job to finish
    wait();
    // lock
    std::lock_guard<std::mutex> lock(output_mtx);
    //sort the output vector base on key values
    std::sort(output_vector.begin(), output_vector.end(),
                [](const OutputPair& a , const OutputPair& b){
                    return *(a.first) < *(b.first);
                });
    return output_vector;
    
}

bool MapReduceJob::isDone(void) const
{
    return is_done.load();
}

MapReduceJob::~MapReduceJob()
{
    wait();
}
