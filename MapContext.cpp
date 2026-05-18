#include "MapContext.h"

MapContext::MapContext(IntermediateVec* intermidiate_vector)
            :intermidate_thread_vec(intermidiate_vector){}


void MapContext::addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value)
{
    intermidate_thread_vec->push_back({key, value});
}
