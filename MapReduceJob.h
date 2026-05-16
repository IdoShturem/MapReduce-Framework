#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H

#include "MapReduceClient.h"
#include <atomic>              
#include <thread>             
#include <mutex>              
#include <condition_variable> 
#include <barrier>             
#include <queue>               
#include <vector>

enum MapReduceStage
{
	UNDEFINED_STAGE, // 0
	MAP_STAGE, // 1
	SHUFFLE_STAGE, // 2
	REDUCE_STAGE // 3
};

class MapReduceState
{
public:
	MapReduceStage stage;
	double percentage;

	inline bool operator==(const MapReduceState &other) const
	{
		return this->stage == other.stage && std::abs(this->percentage - other.percentage) < 1e-6;
	}

	inline bool operator!=(const MapReduceState &other) const
	{
		return !(*this == other);
	}
};

class MapReduceJob
{
public:
	/*
	You CAN NOT change or add properties to this part (public API).
	*/

	MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel);

	~MapReduceJob();

	MapReduceState getState(void) const;

	bool isDone(void) const;
	
	void wait(void);

	OutputVec getOutput(void);

private:
	// references from constructor
	InputVec& input_vector;
	MapReduceClient& client;
	int multy_thread_level;

    // vectors
	std::vector<std::thread> threads;
	std::vector<IntermediateVec> mid_vecs; // private vec for each thread-prevent racing

	// for indexes and state
	std::atomic<size_t> next_input_index;
	std::atomic<uint64_t> job_state;

	// conditions
	std::barrier<> barrier;
	std::condition_variable queue_cnd;

	//mutexes
	std::mutex output_mtx; // reduce sheild
	std::mutex queue_mtx; // for shuffle queue
	std::mutex wait_mtx; // for wait()
	
	//shuffle
	std::queue<IntermediateVec> shuffle_queue;
	
	//output
	OutputVec output_vector;

	// flags
	std::atomic<bool> is_done;
	std::atomic<bool> is_join;


	//friends
	friend class MapContext;
	friend class ReduceContext;
};
	
#endif // MAP_REDUCE_JOB_H
