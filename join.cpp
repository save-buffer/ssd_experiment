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
#include <cstdlib>
#include <iostream>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <liburing.h>

constexpr size_t MAX_MEMORY = 4ull * 1024ull * 1024ull * 1024ull; // 4gb
constexpr size_t NUM_TOTAL_ROWS = 4 * MAX_MEMORY;
constexpr size_t BATCH_SIZE = 1024 * 1024; // 1 million rows
constexpr size_t NUM_TOTAL_BATCHES = NUM_TOTAL_ROWS / BATCH_SIZE;
constexpr size_t PROBE_PER_BUILD_ROWS = 3;
constexpr size_t NUM_PARTITIONS = 16;
constexpr size_t ALIGN = 512;
constexpr size_t MAX_INFLIGHT = 2;
constexpr size_t RING_SIZE = 2 * MAX_INFLIGHT;
const uint32_t NUM_THREADS = std::thread::hardware_concurrency();

uint64_t *AllocateColumn()
{
    return (uint64_t *)aligned_alloc(ALIGN, BATCH_SIZE * sizeof(uint64_t));
}

using Batch = std::pair<uint64_t *, uint64_t *>;
struct BatchBuilder
{
    uint64_t *key()
    {
        if(!batch.first)
            batch.first = AllocateColumn();
        return batch.first;
    }

    uint64_t *pay()
    {
        if(!batch.second)
            batch.second = AllocateColumn();
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

    int inflight = 0;

    int build_fd;
    int probe_fd;
    io_uring ring;
    int ring_fd;
};

std::vector<ThreadLocalData> thread_local_data;
Partition partitions[NUM_PARTITIONS];
std::atomic<int> num_spilled = 0;

std::atomic<State> state;

std::mutex build_mutex;
std::mutex probe_mutex;
std::vector<Batch> build_accum;
std::vector<Batch> probe_accum;
std::atomic<int64_t> memory_used;

void IncMemoryUsage()
{
    memory_used.fetch_add(2 * BATCH_SIZE * sizeof(uint64_t), std::memory_order_relaxed);
}

void DecMemoryUsage()
{
    memory_used.fetch_sub(2 * BATCH_SIZE * sizeof(uint64_t), std::memory_order_relaxed);
}

int64_t GetMemoryUsage()
{
    return memory_used.load(std::memory_order_relaxed);
}

int LockRandomPartition(std::default_random_engine &rng)
{
    for(;;)
    {
        std::uniform_int_distribution<int> dist(0, num_spilled.load() - 1);
        int pid = dist(rng);
        if(partitions[pid].lock.try_lock())
            return pid;
    }
    return -1;
}

Batch GenerateBatch(ThreadLocalData &tld)
{
    std::uniform_int_distribution<uint64_t> dist(0, 512ull * 1024ull * 1024ull);
    uint64_t *key = AllocateColumn();
    uint64_t *pay = AllocateColumn();
    for(uint64_t i = 0; i < BATCH_SIZE; i++)
        key[i] = dist(tld.rng);
    for(uint64_t i = 0; i < BATCH_SIZE; i++)
        pay[i] = dist(tld.rng);
    return { key, pay };
}

template <bool is_build>
bool TryEnqueueBatch(Batch batch)
{
    std::mutex &m = is_build ? build_mutex : probe_mutex;
    std::vector<Batch> &accum = is_build ? build_accum : probe_accum;
    std::lock_guard guard(m);
    if(state.load() != State::Accepting)
        return false;
    accum.push_back(batch);
    IncMemoryUsage();
    return true;
}

void FlushPartition(int pid, BatchBuilder &bb, bool is_build)
{
    std::lock_guard guard(partitions[pid].lock);
    if(is_build)
        partitions[pid].build.push_back(bb.batch);
    else
        partitions[pid].probe.push_back(bb.batch);
    IncMemoryUsage();
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
    free(batch.first);
    free(batch.second);
    DecMemoryUsage();
}

template <bool is_build>
void PartitionImpl(ThreadLocalData &tld)
{
    for(;;)
    {
        Batch batch;
        {
            std::mutex &m = is_build ? build_mutex : probe_mutex;
            std::vector<Batch> &accum = is_build ? build_accum : probe_accum;
            std::lock_guard guard(m);
            if(accum.empty())
                return;
            batch = accum.back();
            accum.pop_back();
        }
        PartitionSingleBatch(batch, tld, is_build);
    }
    for(int p = 0; p < NUM_PARTITIONS; p++)
        FlushPartition(p, tld.partitions[p], is_build);
}

void PartitionBuild(ThreadLocalData &tld)
{
    PartitionImpl<true>(tld);
}

void PartitionProbe(ThreadLocalData &tld)
{
    PartitionImpl<false>(tld);
}

#if 0
void SpillPartitions(ThreadLocalData &tld)
{
    for(;;)
    {
        int pid = LockRandomPartition(tld.rng);
        Partition &p = partitions[pid];
        if(p.inflight == MAX_INFLIGHT)
        {
            p.lock.unlock();
            continue;
        }
        io_uring_sqe *build_sqe = io_uring_get_sqe(&p.ring);
        io_uring_sqe *probe_sqe = io_uring_get_sqe(&p.ring);
        io_uring_prep_write(build_sqe, p.build_fd, BATCH_SIZE * sizeof(uint64_t), 0);
        io_uring_prep_write(probe_sqe, p.probe_fd, BATCH_SIZE * sizeof(uint64_t), 0);
        io_uring_submit(&ring);
        p.inflight++;
    }
}
#endif

template <bool is_build>
void OnInput(Batch batch, ThreadLocalData &tld)
{
    State state_local = state.load();
    switch(state_local)
    {
    case State::Accepting:
    {
        bool queued = TryEnqueueBatch<is_build>(batch);
        if(!queued)
        {
            PartitionSingleBatch(batch, tld, is_build);
        }
        int64_t memory_used = GetMemoryUsage();
        if(memory_used < MAX_MEMORY)
            return;
        State expected = State::Accepting;
        if(state.compare_exchange_strong(expected, State::Spilling))
        {
            size_t b_size;
            size_t p_size;
            {
                std::lock_guard l1(build_mutex);
                b_size = build_accum.size();
            }
            {
                std::lock_guard l2(probe_mutex);
                p_size = probe_accum.size();
            }
            #pragma omp taskloop priority(1)
            for(int i = 0; i < b_size + p_size; i++)
            {
                int tid = omp_get_thread_num();
                bool is_task_build = i < b_size;
                Batch b = is_task_build ? build_accum[i] : probe_accum[i - b_size];
                PartitionSingleBatch(b, thread_local_data[tid], is_task_build);
            }
            // I don't think the guards are needed...
            {
                std::lock_guard l(build_mutex);
                build_accum.clear();
            }
            {
                std::lock_guard l(probe_mutex);
                probe_accum.clear();
            }
        }
    }
    case State::Spilling:
    {
        PartitionSingleBatch(batch, tld, is_build);
    }
    }
}

void OnBuildSide(Batch batch, ThreadLocalData &tld)
{
    OnInput<true>(batch, tld);
}

void OnProbeSide(Batch batch, ThreadLocalData &tld)
{
    OnInput<false>(batch, tld);
}

int main()
{
    thread_local_data.resize(NUM_THREADS);
    for(uint32_t i = 0; i < NUM_THREADS; i++)
    {
        thread_local_data[i].rng.seed(i);
        thread_local_data[i].hashes = AllocateColumn();
    }

    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH | S_IWOTH;
    for(int i = 0; i < NUM_PARTITIONS; i++)
    {
        char build_name[] = "buildX";
        char probe_name[] = "probeX";
        build_name[sizeof(build_name) - 2] = '0' + i;
        probe_name[sizeof(probe_name) - 2] = '0' + i;
        int build_fd;
        int probe_fd;
        if((build_fd = open(build_name, O_DIRECT | O_APPEND | O_LARGEFILE | O_RDWR | O_TRUNC | O_CREAT, mode)) == -1)
        {
            std::cout << "Failed to open " << build_name << ": " << errno << ' ' << strerror(errno) << std::endl;
            return 0;
        }
        if((probe_fd = open(probe_name, O_DIRECT | O_APPEND | O_LARGEFILE | O_RDWR | O_TRUNC | O_CREAT, mode)) == -1)
        {
            std::cout << "Failed to open " << build_name << ": " << errno << ' ' << strerror(errno) << std::endl;
            return 0;
        }
        unlink(build_name);
        unlink(probe_name);

        partitions[i].build_fd = build_fd;
        partitions[i].probe_fd = probe_fd;
        partitions[i].ring_fd = io_uring_queue_init(RING_SIZE, &partitions[i].ring, 0);
    }

    #pragma omp parallel
    #pragma omp single
    #pragma omp taskloop
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
