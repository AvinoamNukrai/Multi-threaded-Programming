// Was written by Avinoam Nukrai, spring 2022

// Imports
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "pthread.h"
#include "Barrier.h"
#include "Barrier.cpp"
#include "iostream"
#include "atomic"
#include "map"
#include "algorithm"
#include "math.h"
using namespace std;

// Constants
#define PTHREAD_CREATE_FAILED "pthread_create function failed!"
#define PTHREAD_JOIN_FAILED "pthread_join function failed!"
#define FAILED_LOCK_MUTEX "mutex lock function failed!"
#define FAILED_UNLOCK_MUTEX "mutex unlock function failed!"
#define EMIT2_FAILED "emit2 function failed"
#define MAP_PHASE_ERROR "map phase function failed"
#define LOCK_MUTEX_ERROR "lock_mutex func failed"
#define UNLOCK_MUTEX_ERROR "unlock mutex function failed"
#define DESTROY_MUTEX_ERROR "destroy_mutex function failed"
#define SYSTEM_ERROR(info) "system error: " << (info) << endl;
#define FAILURE 1
#define SUCCESS 0
#define NUMBER_OF_MUTEXES 8
#define SIZE_M (((uint64_t)(pow(2, 31) - 1)) << 31)
#define COUNT_M (uint64_t) (pow(2, 31) - 1)


/**
 * This struct containes all the mutex needed for the algorithm to run
 */
typedef struct mutexes{
    pthread_mutex_t* map_phase_mutex;
    pthread_mutex_t* emit2_func_mutex;
    pthread_mutex_t* shuffle_phase_mutex;
    pthread_mutex_t* reduce_phase_mutex;
    pthread_mutex_t* emit3_func_mutex;
    pthread_mutex_t* wait_for_job_func_mutex;
    pthread_mutex_t* entry_point_mutex;
    pthread_mutex_t* get_job_state_mutex;

    /**
     * Ctor of the struct
     */
    mutexes(pthread_mutex_t *ptr, pthread_mutex_t *ptr1,
            pthread_mutex_t *ptr2, pthread_mutex_t *ptr3,
            pthread_mutex_t *ptr4, pthread_mutex_t *ptr5,
            pthread_mutex_t *ptr6, pthread_mutex_t *ptr7) {
        map_phase_mutex = ptr;
        emit2_func_mutex = ptr1;
        shuffle_phase_mutex = ptr2;
        reduce_phase_mutex = ptr3;
        emit3_func_mutex = ptr4;
        wait_for_job_func_mutex = ptr5;
        entry_point_mutex = ptr6;
        get_job_state_mutex = ptr7;
    }
    /**
     * Dtor of the sturct (for deallocate all the allocations)
     */
    void destroy_all_mutexes(){
        pthread_mutex_destroy(map_phase_mutex);
        pthread_mutex_destroy(emit2_func_mutex);
        pthread_mutex_destroy(shuffle_phase_mutex);
        pthread_mutex_destroy(wait_for_job_func_mutex);
        pthread_mutex_destroy(entry_point_mutex);
        pthread_mutex_destroy(get_job_state_mutex);
        pthread_mutex_destroy(reduce_phase_mutex);
        pthread_mutex_destroy(emit3_func_mutex);
    }

} mutexes;

/**
 * This struct represent all the context of a single job in the pragram,
 * include array of threads to run the algorithm and data structures where all
 * the daa of the algorithm will be saved. Notice that this struct will be used
 * as the context of each thread of the program, the threads will share the
 * data between them and we will protect this data using mutexes.
 */
typedef struct JobContext{
    const MapReduceClient& client;
    const InputVec& inputVec;
    OutputVec& outputVec;
    map<pthread_t, IntermediateVec*>& intermediateVec_map_by_threads;
    map<K2*, IntermediateVec*>& shuffle_vectors_by_keys;
    int multiThreadLevel;
    Barrier* barrier;
    pthread_t* threads;
    mutexes* all_mutexes;
    pthread_t thread_zero; // for the shuffle phase
    bool finished; // indicates if the job is finished or not
    std::atomic<unsigned long int>* mapCounter;
    std::atomic<unsigned long int>* doneMapping;
    std::atomic<uint64_t> ac;
    std::atomic<uint64_t> percentage_ac;
} JobContext;

/**
 * declaration of the threads entry point (same one for all of them)
 */
void* entry_point(void* arg);

/**
 * This function responsible for start the algorithm, meaning that all the
 * allocations and threads creation will be done here.
 * @param client - the MapReduceClient object that contains the implementation
 * of the map, reduce functions of the task
 * @param inputVec - the input vector in which we will work on
 * @param outputVec - the vector in which we will store the output data
 * @param multiThreadLevel - the number of threads we want for running the algorithm
 * @return JobHandle - void*
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    auto threads = new pthread_t[multiThreadLevel];
    auto barrier = new Barrier(multiThreadLevel);
    auto inter_map_by_threads = new map<pthread_t, IntermediateVec*>;
    auto shuffeld_map = new map<K2*, IntermediateVec*>;
    auto mapAtomic = new std::atomic<unsigned long int>(0);
    auto doneMapping = new std::atomic<unsigned long int>(0);
    auto mu = new mutexes(new pthread_mutex_t , new pthread_mutex_t,
                              new pthread_mutex_t , new pthread_mutex_t,
                              new pthread_mutex_t , new pthread_mutex_t,
                              new pthread_mutex_t, new pthread_mutex_t);

    auto jobContext = new JobContext{client, inputVec, outputVec,
                                     *inter_map_by_threads, *shuffeld_map,
                                     multiThreadLevel, barrier, threads,
                                     mu, .finished = false,
                                     mapAtomic, doneMapping};
    jobContext->percentage_ac = 0;
    jobContext->ac = 0;
    // create threads
    for (int i = 0; i < multiThreadLevel; i++){
        if (pthread_create(&threads[i], NULL, entry_point, jobContext) != 0){
            cerr << SYSTEM_ERROR(PTHREAD_CREATE_FAILED)
            // Creating thread falied -> deallocate all allocated memory
            delete[] threads;
            delete barrier;
            delete inter_map_by_threads;
            delete shuffeld_map;
            delete mapAtomic;
            delete doneMapping;
            jobContext->all_mutexes->destroy_all_mutexes();
            delete jobContext;
            exit(FAILURE);
        }
    }
    JobHandle jobHandle = (JobHandle*)jobContext;
    return jobHandle;
}

/**
 * This func lock a given mutex and checks validation of the lock process
 * @param some_mutex - pointer to pthread_mutex_t
 * @return binary value of success or failure
 */
int lock_mutex(pthread_mutex_t* some_mutex){
    if (pthread_mutex_lock(some_mutex)){
        cerr << SYSTEM_ERROR(FAILED_LOCK_MUTEX);
        return FAILURE;
    }
    return SUCCESS;
}

/**
* This func unlock a given mutex and checks validation of the unlock process
* @param some_mutex - pointer to pthread_mutex_t
* @return binary value of success or failure
*/
int unlock_mutex(pthread_mutex_t* some_mutex){
    if (pthread_mutex_unlock(some_mutex)){
        cerr << SYSTEM_ERROR(FAILED_UNLOCK_MUTEX);
        return FAILURE;
    }
    return SUCCESS;
}

/**
 * This func will 'wait' for a given job to be done - act the pthread_join func
 * of each thread of the given job
 * @param job
 */
void waitForJob(JobHandle job){
    JobContext* jobContext = (JobContext*)job;
    lock_mutex(jobContext->all_mutexes->wait_for_job_func_mutex);
    for (int i = 0; i < jobContext->multiThreadLevel; ++i) {
        pthread_join(jobContext->threads[i], NULL);
    }
    jobContext->finished = true;
    unlock_mutex(jobContext->all_mutexes->wait_for_job_func_mutex);
}

/**
 * This func load the current state of a given job into some given state
 * @param job - JobHandle object for load from
 * @param state - state object for load to
 */
void getJobState(JobHandle job, JobState* state){
    auto jobContext = (JobContext*)job;
    lock_mutex(jobContext->all_mutexes->get_job_state_mutex);
    // update stage
    state->stage = (stage_t)((size_t)jobContext->percentage_ac.load() >> 62);
    // update percentage
    if(jobContext->percentage_ac >> 62 == MAP_STAGE){
        auto cur_perc = 100 * ((float) (*(jobContext->doneMapping)+1)
            / (float) (jobContext->inputVec.size()));
        if (cur_perc <= 100){
            state->percentage = 100 * ((float) (*(jobContext->doneMapping)+1)
                / (float) (jobContext->inputVec.size()));
            unlock_mutex(jobContext->all_mutexes->get_job_state_mutex);
            return;
        }
        state->percentage = 100 * ((float) (*(jobContext->doneMapping)) /
            (float) (jobContext->inputVec.size()));
        unlock_mutex(jobContext->all_mutexes->get_job_state_mutex);
        return;
    }
    auto counter = (float) (((size_t)jobContext->percentage_ac) & COUNT_M); // number of jobs that already done
    auto size = (float)((((size_t)jobContext->percentage_ac & SIZE_M) >> 31)); // number of those that are waiting
    if (size == 0){state->percentage = 0;}
    else { state->percentage = 100 * (counter / size);}
    unlock_mutex(jobContext->all_mutexes->get_job_state_mutex);
}

/**
 * This func is responsible for closing some given job, that is deallocate all
 * the allocate data for the given job. The func will do so only if the given
 * job was finished only, if not - we will wait till it finished.
 * @param job - JobHandle object to close
 */
void closeJobHandle(JobHandle job){
    // input validation
    JobContext* jobContext = (JobContext*) job;
    if (job == nullptr) exit(FAILURE);
    // check if the job was finished or not, if not we will wait till then
    if (!jobContext->finished){
        waitForJob(job);
    }
    // deallocate all the dynamic alloctions
    delete[] jobContext->threads;
    jobContext->threads = nullptr;
    delete jobContext->barrier;
    jobContext->barrier = nullptr;
    for (auto& pair: jobContext->intermediateVec_map_by_threads) {
        delete pair.second;
    }
    delete &jobContext->intermediateVec_map_by_threads;
    for (auto& pair: jobContext->shuffle_vectors_by_keys) {
        delete pair.second;
    }
    delete &jobContext->shuffle_vectors_by_keys;
    delete jobContext->mapCounter;
    jobContext->mapCounter = nullptr;
    delete jobContext->doneMapping;
    jobContext->doneMapping = nullptr;
    jobContext->all_mutexes->destroy_all_mutexes();
    delete jobContext;
}

/**
 * This func responsible for write the given pair (intermidiate pair)
 * into the given context. Will be called form the client.map func.
 * @param key - K2*, key of the pair
 * @param value V2*, value of the pair
 * @param context - JobHandle to write to
 */
void emit2 (K2* key, V2* value, void* context){
    auto jobContext = (JobContext*) context;
    lock_mutex(jobContext->all_mutexes->emit2_func_mutex);
    pthread_t current_id = pthread_self(); // returns the id of the cur thread
    auto iter = jobContext->intermediateVec_map_by_threads.find(current_id);
    if (iter == jobContext->intermediateVec_map_by_threads.end()){
        jobContext->intermediateVec_map_by_threads[current_id] = new IntermediateVec;
        jobContext->intermediateVec_map_by_threads[current_id]->push_back(IntermediatePair(key, value));
        unlock_mutex(jobContext->all_mutexes->emit2_func_mutex);
        return;
    }
    jobContext->intermediateVec_map_by_threads[current_id]->push_back(IntermediatePair(key, value));
    unlock_mutex(jobContext->all_mutexes->emit2_func_mutex);
}

/**
 * This func responsible for write the given pair (output pair)
 * into the given context. Will be called from the client.reduce func
 * @param key - K3*, key of the pair
 * @param value V3*, value of the pair
 * @param context - JobHandle to write to
 */
void emit3 (K3* key, V3* value, void* context){
    auto jobContext = (JobContext*) context;
    lock_mutex(jobContext->all_mutexes->emit3_func_mutex);
    jobContext->outputVec.push_back(OutputPair(key, value));
    unlock_mutex(jobContext->all_mutexes->emit3_func_mutex);
}

/**
 * This func is the map phase func of the algorithm, responsible for act the
 * map func of he cliend onto the input elements.
 * @param jobContext - JobContext object to work from.
 */
void map_phase(JobContext* jobContext){
    lock_mutex(jobContext->all_mutexes->get_job_state_mutex);
    // Update the stage to be MAP_STAGE
    if ((jobContext->percentage_ac >> 62) == UNDEFINED_STAGE){
        auto stage = 1UL << 62;
        auto size = (((uint64_t)jobContext->inputVec.size()) << 31);
        auto atomic = stage + size;
        jobContext->percentage_ac = atomic;
    }
    unsigned long curr = (*(jobContext->mapCounter))++;
    unlock_mutex(jobContext->all_mutexes->get_job_state_mutex);
    while (curr < jobContext->inputVec.size()){
        lock_mutex(jobContext->all_mutexes->map_phase_mutex);
        jobContext->client.map(jobContext->inputVec[curr].first,
                               jobContext->inputVec[curr].second, jobContext);
        (*(jobContext->doneMapping))++;
        unlock_mutex(jobContext->all_mutexes->map_phase_mutex);
        lock_mutex(jobContext->all_mutexes->get_job_state_mutex);
        curr = (*(jobContext->mapCounter))++;
        unlock_mutex(jobContext->all_mutexes->get_job_state_mutex);
    }

}

/**
 * This func is the sort phase of the algorithm, sort the intermidiate vectors
 * by the K2* keys.
 * @param jobContext - context to sort from
 */
void sort_per_thread(JobContext& jobContext){
    pthread_t cur_id = pthread_self();
    std::sort(jobContext.intermediateVec_map_by_threads[cur_id]->begin(),
              jobContext.intermediateVec_map_by_threads[cur_id]->end());
}

/**
 * This func is the shuffle phase of the algorithm, responsible for creates a
 * shuffled map, will be called after the sort phase (much easy).
 * @param jobContext - jobContext to shuffle from and into it
 */
void shuffle_phase(JobContext& jobContext){
    lock_mutex(jobContext.all_mutexes->get_job_state_mutex);
    // Update the stage to be SHUFFLE_STAGE
    if ((jobContext.percentage_ac >> 62) == MAP_STAGE){
        auto stage = 2UL << 62;
        auto size = (((uint64_t)jobContext.intermediateVec_map_by_threads.size()) << 31);
        auto atomic = stage + size;
        jobContext.percentage_ac = atomic;
    }
    unlock_mutex(jobContext.all_mutexes->get_job_state_mutex);
    lock_mutex(jobContext.all_mutexes->shuffle_phase_mutex);
    for (auto& iter_inters: jobContext.intermediateVec_map_by_threads){
        auto cur_inter_vec = iter_inters.second;
        if (!cur_inter_vec->empty()){
            for (auto& iter_vec: *cur_inter_vec){
                bool if_found = false;
                if (jobContext.shuffle_vectors_by_keys.empty()){
                    auto* vec = new IntermediateVec();
                    vec->push_back(IntermediatePair(iter_vec.first,
                                                    iter_vec.second));
                    jobContext.shuffle_vectors_by_keys[iter_vec.first] = vec;
                    continue;
                }
                for (auto& e: jobContext.shuffle_vectors_by_keys) {
                    if(!(e.first->operator<(*iter_vec.first)) &&
                    !(iter_vec.first->operator<(*e.first))){
                        // the key is already in the shuffle_vectors_by_keys
                        jobContext.shuffle_vectors_by_keys[e.first]->push_back(iter_vec);
                        if_found = true;
                        break;
                    }
                }
                if (!if_found){
                    // else if the key is not in shuffle_vectors_by_keys ->
                    // we want to add it by allocate new vec
                    auto* vec = new IntermediateVec();
                    vec->push_back(IntermediatePair(iter_vec.first, iter_vec.second));
                    jobContext.shuffle_vectors_by_keys[iter_vec.first] = vec;
                }
            }
            jobContext.percentage_ac++;
        }
    }
    unlock_mutex(jobContext.all_mutexes->shuffle_phase_mutex);
}

/**
 * This is the reduce phase of the algorithm, responsible to act the
 * client.reduce func on the intermidiate elements.
 * @param jobContext - jobContext to reduce from and into it
 */
void reduce_phase(JobContext& jobContext){
    lock_mutex(jobContext.all_mutexes->get_job_state_mutex);
    // Update the stage to e REDUCE STAGE
    if ((jobContext.percentage_ac >> 62) == SHUFFLE_STAGE){
        auto stage = 3UL << 62;
        auto size = (((uint64_t)jobContext.shuffle_vectors_by_keys.size()) << 31);
        auto atomic = stage + size;
        jobContext.percentage_ac = atomic;
    }
    unlock_mutex(jobContext.all_mutexes->get_job_state_mutex);
    lock_mutex(jobContext.all_mutexes->reduce_phase_mutex);
    auto vec = new vector<K2*>;
    for (const auto& iter:jobContext.shuffle_vectors_by_keys) {
        jobContext.client.reduce(iter.second, &jobContext);
        vec->push_back(iter.first);
        jobContext.percentage_ac++;
    }
    for (auto& k:*vec){
        jobContext.shuffle_vectors_by_keys.erase(k);
    }
    delete vec;
    unlock_mutex(jobContext.all_mutexes->reduce_phase_mutex);
}

/**
 * This func is the entry_point of the threads of the algorithm, each thread
 * will be running this func in turn.
 * @param arg - arg of the thread
 * @return the thread context
 */
void* entry_point(void* arg){
    auto jobContext = (JobContext*)arg;
    lock_mutex(jobContext->all_mutexes->entry_point_mutex);
    if(jobContext->intermediateVec_map_by_threads.empty()){
        jobContext->thread_zero = pthread_self();
    }
    jobContext->intermediateVec_map_by_threads[pthread_self()] =
        new IntermediateVec();
    unlock_mutex(jobContext->all_mutexes->entry_point_mutex);
    map_phase(jobContext); // do map phase
    sort_per_thread(*jobContext); // do sort phase
    jobContext->barrier->barrier(); // do barrier
    if(jobContext->thread_zero == pthread_self()){
        shuffle_phase(*jobContext); // do shuffle_phase phase
    }
    jobContext->barrier->barrier(); // do barrier
    reduce_phase(*jobContext); // do reduce phase
    return jobContext;
}