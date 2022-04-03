one_file: params.h one_file.cpp Makefile
	g++ -O3 one_file.cpp -o one_file -luring -lpthread -lomp -ggdb -fopenmp

many_file: params.h many_file.cpp Makefile
	g++ -O3 many_file.cpp -o many_file -luring -lpthread -lomp -ggdb -fopenmp

join: join.cpp Makefile
	g++ -std=gnu++17 -O3 join.cpp -o join -luring -lpthread -lomp -ggdb -fopenmp

all: one_file many_file join

.DEFAULT_GOAL := all

.PHONY: fio fio_many_file fio_one_file

fio_many_file:
	rm -f write* && fio --name=write --ioengine=io_uring --rw=write --bs=1m --size=256m --numjobs=16 --direct=1

fio_one_file:
	rm -f write* && fio --name=write --ioengine=io_uring --rw=write --bs=1m --size=4g --direct=1

fio: fio_many_file fio_one_file
