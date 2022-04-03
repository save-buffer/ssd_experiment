#include <omp.h>
#include <atomic>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <utility>
#include <sys/resource.h>

constexpr size_t MAX_MEMORY = 4ull * 1024ull * 1024ull * 1024ull; // 4gb
constexpr size_t NUM_TOTAL_ROWS = 4 * MAX_MEMORY;
constexpr size_t BATCH_SIZE = 1024 * 1024; // 1 million rows
constexpr size_t NUM_TOTAL_BATCHES = NUM_TOTAL_ROWS / BATCH_SIZE;
constexpr size_t PROBE_PER_BUILD_ROWS = 3;
constexpr size_t NUM_PARTITIONS = 16;
const uint32_t NUM_THREADS = std::thread::hardware_concurrency();

using Batch = std::pair<uint64_t *, uint64_t *>;

enum class State
{
    Accepting,
    PartitioningBuild,
    PartitioningProbe,
    Spilling,
};

struct ThreadLocalData
{
    std::default_random_engine rng;
    bool build_done = false;
    bool probe_done = false;
};

struct Partition
{
    std::mutex lock;
    std::vector<Batch> accum;
};

std::vector<ThreadLocalData> thread_local_data;
Partition partitions[NUM_PARTITIONS];

std::atomic<State> state;

std::mutex build_mutex;
std::mutex probe_mutex;
uint64_t build_generated = 0;
uint64_t probe_generated = 0;
std::vector<Batch> build_accum;
std::vector<Batch> probe_accum;

Batch GenerateBatch(ThreadLocalData &tld)
{
    std::uniform_int_distribution<uint64_t> dist(0, 512ull * 1024ull * 1024ull);
    uint64_t *key = (uint64_t *)malloc(BATCH_SIZE * sizeof(uint64_t));
    uint64_t *pay = (uint64_t *)malloc(BATCH_SIZE * sizeof(uint64_t));
    for(uint64_t i = 0; i < BATCH_SIZE; i++)
        key[i] = dist(tld.rng);
    for(uint64_t i = 0; i < BATCH_SIZE; i++)
        pay[i] = dist(tld.rng);
    return { key, pay };
}

void OnBuildSide(Batch batch)
{
retry:
    State state_local = state.load();
    switch(state_local)
    {
    case State::Accepting:
    {
        rusage usage;
        getrusage(RUSAGE_SELF, &usage);
        int64_t memory_used = usage.ru_maxrss;
        if(memory_used >= MAX_MEMORY)
        {
            State expected = State::Accepting;
            state.compare_exchange_strong(expected, State::PartitioningBuild);
            goto retry;
        }
        
    }
    break;
    case State::PartitioningBuild:
        break;
    case State::PartitioningProbe:
        break;
    case State::Spilling:
        break;
    }
}

int main()
{
    thread_local_data.resize(NUM_THREADS);
    for(uint32_t i = 0; i < NUM_THREADS; i++)
        thread_local_data[i].rng.seed(i);

    #pragma omp parallel for
    for(size_t ibatch = 0; ibatch < NUM_TOTAL_BATCHES; ibatch++)
    {
        std::uniform_int_distribution<int> is_probe(0, PROBE_PER_BUILD_ROWS); // 0 if build

        int tid = omp_get_thread_num();
        ThreadLocalData &tld = thread_local_data[tid];
        Batch batch = GenerateBatch(tld);
        if(is_probe)
            OnProbeSide(batch);
        else
            OnBuildSide(batch);
    }
    return 0;
}
