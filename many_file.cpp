#include <liburing.h>
#include <omp.h>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>
#include <mutex>
#include <thread>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include "params.h"

int main()
{
    uint32_t num_threads = std::thread::hardware_concurrency();
    size_t gb = file_size / (1024 * 1024 * 1024);

    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH | S_IWOTH;
    double start_write, end_write;
    double start_read, end_read;
    double start_read_vectored, end_read_vectored;
#pragma omp parallel
    {
        int tid = omp_get_thread_num();
        char name[] = "borkX";
        name[sizeof(name) - 2] = '0' + tid;
        int fd;
        if((fd = open(name, sync_flag | O_APPEND | O_LARGEFILE | O_RDWR | O_TRUNC | O_CREAT, mode)) == -1)
        {
            std::cout << "Failed to open " << name << ": " << errno << ' ' << strerror(errno) << std::endl;
        }
        unlink(name);
        size_t offset = 0;
        void *buf = malloc(chunk_size + align);
        buf = (void *)(((uintptr_t)buf + (align - 1)) & -align);
        memset(buf, tid, chunk_size);
        io_uring ring;
        int ring_fd = io_uring_queue_init(max_in_flight, &ring, 0);

#pragma omp barrier
#pragma omp master
        start_write = omp_get_wtime();

        int inflight = 0;
        for(size_t offset = 0; offset < (file_size / num_threads); offset += chunk_size)
        {
            if(inflight == max_in_flight)
            {
                io_uring_cqe *cqe;
                int ret = io_uring_wait_cqe(&ring, &cqe);
                if(ret < 0)
                {
                    std::cout << "Fail wait\n";
                    break;
                }
                if(cqe->res < 0)
                {
                    std::cout << "Fail cqe: " << cqe->res << ' ' << strerror(-cqe->res) << "\n";
                    break;
                }
                io_uring_cqe_seen(&ring, cqe);
                inflight--;
            }
            io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            io_uring_prep_write(sqe, fd, buf, chunk_size, 0);
            io_uring_submit(&ring);
            inflight++;
        }
        io_uring_cqe *cqe_final;
        io_uring_wait_cqe_nr(&ring, &cqe_final, inflight);
#pragma omp barrier
#pragma omp master
        end_write = omp_get_wtime();
        io_uring_cqe_seen(&ring, cqe_final);
        for(int i = 0; i < inflight - 1; i++)
        {
            io_uring_wait_cqe(&ring, &cqe_final);
            io_uring_cqe_seen(&ring, cqe_final);
        }
        size_t num_chunks = file_size / (num_threads * chunk_size);
        std::vector<void *> chunks(num_chunks);
        for(void *&ptr : chunks)
        {
            ptr = malloc(chunk_size + align);
            ptr = (void *)(((uintptr_t)ptr + (align - 1)) & -align);
        }
#pragma omp barrier
#pragma omp master
        start_read = omp_get_wtime();
        inflight = 0;
        for(size_t offset = 0; offset < (file_size / num_threads); offset += chunk_size)
        {
            if(inflight == max_in_flight)
            {
                io_uring_cqe *cqe;
                int ret = io_uring_wait_cqe(&ring, &cqe);
                if(ret < 0)
                {
                    std::cout << "Fail wait\n";
                    break;
                }
                if(cqe->res < 0)
                {
                    std::cout << "Fail cqe: " << cqe->res << ' ' << strerror(-cqe->res) << "\n";
                    break;
                }
                io_uring_cqe_seen(&ring, cqe);
                inflight--;
            }
            io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            io_uring_prep_read(sqe, fd, buf, chunk_size, offset);
            io_uring_submit(&ring);
            inflight++;
        }
#pragma omp barrier
#pragma omp master
        end_read = omp_get_wtime();
        io_uring_cqe_seen(&ring, cqe_final);
        for(int i = 0; i < inflight - 1; i++)
        {
            io_uring_wait_cqe(&ring, &cqe_final);
            io_uring_cqe_seen(&ring, cqe_final);
        }
        std::vector<iovec> iovs(num_chunks);
        for(size_t i = 0; i < num_chunks; i++)
        {
            iovs[i].iov_base = chunks[i];
            iovs[i].iov_len = chunk_size;
        }
#pragma omp barrier
#pragma omp master
        start_read_vectored = omp_get_wtime();
        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        io_uring_prep_readv(sqe, fd, iovs.data(), iovs.size(), 0);
        io_uring_submit(&ring);
        io_uring_wait_cqe(&ring, &cqe_final);
#pragma barrier
#pragma omp master
        end_read_vectored = omp_get_wtime();
    }
    double time_write = end_write - start_write;
    double time_read = end_read - start_read;
    double time_read_vectored = end_read_vectored - start_read_vectored;
    double gb_per_sec_write = gb / time_write;
    double gb_per_sec_read = gb / time_read;
    double gb_per_sec_read_vectored = gb / time_read_vectored;
    std::cout << "Write File-per-thread (SyncFlag = " << sync_str << ") " << gb << "GB took " << time_write << " s with " << num_threads << " threads (" << gb_per_sec_write << "GB/s)" << std::endl;
    std::cout << "Read File-per-thread (SyncFlag = " << sync_str << ") " << gb << "GB took " << time_read << " s with " << num_threads << " threads (" << gb_per_sec_read << "GB/s)" << std::endl;
    std::cout << "Vectored Read File-per-thread (SyncFlag = " << sync_str << ") " << gb << "GB took " << time_read_vectored << " s with " << num_threads << " threads (" << gb_per_sec_read_vectored << "GB/s)" << std::endl;
    return 0;
}
