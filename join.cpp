#include <omp.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <utility>
#include <functional>
#include <sys/resource.h>

constexpr size_t MAX_MEMORY = 4ull * 1024ull * 1024ull * 1024ull; // 4gb
constexpr size_t NUM_TOTAL_ROWS = 4 * MAX_MEMORY;
constexpr size_t BATCH_SIZE = 1024 * 1024; // 1 million rows
constexpr size_t NUM_TOTAL_BATCHES = NUM_TOTAL_ROWS / BATCH_SIZE;
constexpr size_t PROBE_PER_BUILD_ROWS = 3;
constexpr size_t NUM_PARTITIONS = 16;
const uint32_t NUM_THREADS = std::thread::hardware_concurrency();

using Batch = std::pair<uint64_t *, uint64_t *>;
struct BatchBuilder
{
    uint64_t *key()
    {
        if(!batch.first)
            batch.first = (uint64_t *)malloc(BATCH_SIZE * sizeof(uint64_t));
        return batch.first;
    }

    uint64_t *pay()
    {
        if(!batch.second)
            batch.second = (uint64_t *)malloc(BATCH_SIZE * sizeof(uint64_t));
        return batch.second;
    }

    bool IsFull()
    {
        return length == BATCH_SIZE;
    }

    Batch batch = { nullptr, nullptr };
    uint64_t length = 0;
};

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
    BatchBuilder partitions[NUM_PARTITIONS];
    uint64_t *hashes;
};

struct Partition
{
    std::mutex lock;
    std::vector<Batch> build;
    std::vector<Batch> probe;
    int64_t num_rows = 0;
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

void EnqueueBatch(Batch batch, std::vector<Batch> &accum)
{
    accum.push_back(batch);
}

void EnqueueBatch_Build(Batch batch)
{
    std::lock_guard guard(build_mutex);
    EnqueueBatch(batch, build_accum);
}

void EnqueueBatch_Probe(Batch batch)
{
    std::lock_guard guard(probe_mutex);
    EnqueueBatch(batch, probe_accum);
}

void FlushPartition(int pid, BatchBuilder &bb, bool is_build)
{
    std::lock_guard guard(partitions[pid].lock);
    if(is_build)
        partitions[pid].build.push_back(bb.batch);
    else
        partitions[pid].probe.push_back(bb.batch);
    bb.length = 0;
    bb.batch = { nullptr, nullptr };
}

void AppendBatchToPartitions(Batch batch, ThreadLocalData &tld, const int *partition_starts, bool is_build)
{
    for(int p = 0; p < NUM_PARTITIONS; p++)
    {
        BatchBuilder &bb = tld.partitions[p];
        uint64_t num_to_copy = partition_starts[p + 1] - partition_starts[p];
        int copy_into_this_batch = std::min(num_to_copy, BATCH_SIZE - bb.length);
        std::memcpy(bb.key() + bb.length, batch.first + partition_starts[p], copy_into_this_batch);
        std::memcpy(bb.pay() + bb.length, batch.second + partition_starts[p], copy_into_this_batch);
        bb.length += num_to_copy;
        if(bb.IsFull())
        {
            FlushPartition(p, bb, is_build);
            num_to_copy -= copy_into_this_batch;
            std::memcpy(bb.key(), batch.first + partition_starts[p] + copy_into_this_batch, num_to_copy);
            std::memcpy(bb.pay(), batch.second + partition_starts[p] + copy_into_this_batch, num_to_copy);
            bb.length = num_to_copy;
        }
    }
}

void PartitionSingleBatch(Batch batch, ThreadLocalData &tld, bool is_build)
{
    std::transform(batch.first, batch.first + BATCH_SIZE, tld.hashes, std::hash<uint64_t>{});
    std::sort(batch.first, batch.first + BATCH_SIZE, [&](const uint64_t &a, const uint64_t &b)
    {
        int a_idx = &a - batch.first;
        int b_idx = &b - batch.first;
        uint64_t pid_a = tld.hashes[a_idx] % NUM_PARTITIONS;
        uint64_t pid_b = tld.hashes[b_idx] % NUM_PARTITIONS;
        return pid_a < pid_b;
    });
    std::sort(batch.second, batch.second + BATCH_SIZE, [&](const uint64_t &a, const uint64_t &b)
    {
        int a_idx = &a - batch.second;
        int b_idx = &b - batch.second;
        uint64_t pid_a = tld.hashes[a_idx] % NUM_PARTITIONS;
        uint64_t pid_b = tld.hashes[b_idx] % NUM_PARTITIONS;
        return pid_a < pid_b;
    });
    std::sort(tld.hashes, tld.hashes + BATCH_SIZE, [&](const uint64_t &a, const uint64_t &b)
    {
        return (a % NUM_PARTITIONS) < (b % NUM_PARTITIONS);
    });
    int partition_starts[NUM_PARTITIONS + 1];
    int irow = 0;
    for(int i = 1; i < NUM_PARTITIONS; i++)
    {
        while((tld.hashes[irow] % NUM_PARTITIONS) < i)
            irow++;
        partition_starts[i] = irow;
    }
    partition_starts[NUM_PARTITIONS] = BATCH_SIZE;
    AppendBatchToPartitions(batch, tld, partition_starts, is_build);
}

void PartitionBuild(ThreadLocalData &tld)
{
    for(;;)
    {
        Batch batch;
        {
            std::lock_guard guard(build_mutex);
            if(build_accum.empty())
                return;
            batch = build_accum.back();
            build_accum.pop_back();
        }
        PartitionSingleBatch(batch, tld, /*is_build*/ true);
    }
}

void PartitionProbe(ThreadLocalData &tld)
{
}

void OnBuildSide(Batch batch, ThreadLocalData &tld)
{
    State state_local = state.load();
    switch(state_local)
    {
    case State::Accepting:
    {
        EnqueueBatch_Build(batch);
        rusage usage;
        getrusage(RUSAGE_SELF, &usage);
        int64_t memory_used = usage.ru_maxrss;
        if(memory_used < MAX_MEMORY)
        {
            return;
        }
        State expected = State::Accepting;
        state.compare_exchange_strong(expected, State::PartitioningBuild);
    }
    // Fall through
    case State::PartitioningBuild:
        PartitionBuild(tld);
        // Fall through
    case State::PartitioningProbe:
        PartitionProbe(tld);
        // Fall through
    case State::Spilling:
        break;
    }
}

void OnProbeSide(Batch batch, ThreadLocalData &tld)
{
}

int main()
{
    thread_local_data.resize(NUM_THREADS);
    for(uint32_t i = 0; i < NUM_THREADS; i++)
    {
        thread_local_data[i].rng.seed(i);
        thread_local_data[i].hashes = (uint64_t *)malloc(BATCH_SIZE * sizeof(uint64_t));
    }

    #pragma omp parallel for
    for(size_t ibatch = 0; ibatch < NUM_TOTAL_BATCHES; ibatch++)
    {
        std::uniform_int_distribution<int> is_probe(0, PROBE_PER_BUILD_ROWS); // 0 if build

        int tid = omp_get_thread_num();
        ThreadLocalData &tld = thread_local_data[tid];
        Batch batch = GenerateBatch(tld);
        if(is_probe(tld.rng))
            OnProbeSide(batch, tld);
        else
            OnBuildSide(batch, tld);
    }
    return 0;
}
