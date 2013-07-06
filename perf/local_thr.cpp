/*
 Copyright (c) 2007-2012 iMatix Corporation
 Copyright (c) 2009-2011 250bpm s.r.o.
 Copyright (c) 2007-2011 Other contributors as noted in the AUTHORS file

 This file is part of 0MQ.

 0MQ is free software; you can redistribute it and/or modify it under
 the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation; either version 3 of the License, or
 (at your option) any later version.

 0MQ is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Lesser General Public License for more details.

 You should have received a copy of the GNU Lesser General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * local_thr.cpp is a program for measuring zmq throughput between two hosts.
 * It needs remote_thr running on one host and local_thr on the other. The
 * bind address is determined by the host on which local_thr is running.
 *
 * This is a modified version of local_thr.cpp to measure performance of
 * zmq and writing the data transferred to disk. The disk writing is delegated
 * to a thread in order to parallelize the network transfer and file writing.
 * The number of threads is 1 by default but can be increased up to 10.
 * The path of the disk is specified as a parameter. If /network is specified
 * then no file is written on only the data transfer is measured.
 * A counter is sent in the first 4 bytes and checked to know if any packets
 * have lost.
 *
 * Andy Götz (ESRF) , 6 July 2013
 */
#include "../include/zmq.h"
#include "../include/zmq_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>

#define MAX_THREADS 10

static int thread_ctr = 0;
static char *disk_name;

/*
 * file_writer(): a thread to write data to disk. The data to be written is
 * transferred as the input parameter in a zmq_msg.
 * The file name is made of the disk name + /data/test + thread_counter
 * thread_counter is a monotonically incremented counter.
 * If disk name == /network no file is written.
 * The zmq_msg data is freed at the end to avoid a memory leak.
 *
 * Andy Götz (ESRF) , 6 July 2013
 */
void *file_writer(void *msg_copy) {
	char fn[1024];
	FILE * fd;
	zmq_msg_t *msg = (zmq_msg_t*) msg_copy;

	if (strcmp(disk_name, "/network") != 0) {
		sprintf(fn, "%s/data/test%06d.dat", disk_name, thread_ctr);
		fd = fopen(fn, "wc");
		if (fd == NULL) {
			printf("file_writer(): failed to open file %s\n", fn);
			return 0;
		}
		fwrite(zmq_msg_data(msg), 1, zmq_msg_size(msg), fd);
		fclose(fd);
	}
	zmq_msg_close(msg);
	free(msg);
	return 0;
}

/*
 * main() - program to setup a ZMQ pull socket, receive zmq messages, create
 * a thread and pass a message to it, measure the performance and print out
 * the values on stdout in a format so they can be plotted (e.g. gnuplot)
 *
 * Andy Götz (ESRF) , 6 July 2013
 *
 */
int main(int argc, char *argv[]) {
	const char *bind_to;
	int message_count;
	size_t message_size;
	void *ctx;
	void *s;
	int rc;
	int i;
	zmq_msg_t msg;
	void *watch, *watch_1000;
	unsigned long elapsed;
	unsigned long throughput;
	double megabits, megabytes;
	int msg_counter, new_msg_counter;
	char hostname[80];
	int no_threads;

	if (argc < 5) {
		printf(
				"usage: local_thr <bind-to> <message-size> <message-count> <disk-mountpoint> [no_threads]\n");
		return 1;
	}
	gethostname(hostname, sizeof(hostname));
	bind_to = argv[1];
	message_size = atoi(argv[2]);
	message_count = atoi(argv[3]);
	disk_name = argv[4];
	no_threads = 1;
	if (argc > 5) {
	    no_threads = atoi(argv[5]);
	    if (no_threads > MAX_THREADS)
		no_threads = MAX_THREADS;
        }
	printf(
			"#local_thr local host %s disk %s bind to %s message size %s message count %s writer threads %d\n",
			hostname, argv[4], argv[1], argv[2], argv[3], no_threads);

	ctx = zmq_init(1);
	if (!ctx) {
		printf("error in zmq_init: %s\n", zmq_strerror(errno));
		return -1;
	}

	s = zmq_socket(ctx, ZMQ_PULL);
	if (!s) {
		printf("error in zmq_socket: %s\n", zmq_strerror(errno));
		return -1;
	}

	//  Add your socket options here.
	//  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

	rc = zmq_bind(s, bind_to);
	if (rc != 0) {
		printf("error in zmq_bind: %s\n", zmq_strerror(errno));
		return -1;
	}

	rc = zmq_msg_init(&msg);
	if (rc != 0) {
		printf("error in zmq_msg_init: %s\n", zmq_strerror(errno));
		return -1;
	}

	rc = zmq_recvmsg(s, &msg, 0);
	if (rc < 0) {
		printf("error in zmq_recvmsg: %s\n", zmq_strerror(errno));
		return -1;
	}
	if (zmq_msg_size(&msg) != message_size) {
		printf("message of incorrect size (%d) received\n",
				(int) zmq_msg_size(&msg));
		//return -1;
	}
	msg_counter = *(int*) zmq_msg_data(&msg);

	watch = zmq_stopwatch_start();
	watch_1000 = zmq_stopwatch_start();

	pthread_t file_writer_thread[MAX_THREADS];
	for (i = 0; i < MAX_THREADS; i++) {
		file_writer_thread[i] = 0;
	}

	printf("#mean throughput for 1000 msg: \n");
	for (i = 0; i != message_count - 1; i++) {
		zmq_msg_t *msg_copy;
		rc = zmq_recvmsg(s, &msg, 0);
		if (rc < 0) {
			printf("error in zmq_recvmsg: %s\n", zmq_strerror(errno));
			return -1;
		}
		if (zmq_msg_size(&msg) != message_size) {
			printf("message of incorrect size (%d) received\n",
					(int) zmq_msg_size(&msg));
			//return -1;
		}
		new_msg_counter = *(int*) zmq_msg_data(&msg);

		 if ((new_msg_counter - msg_counter) != 1) {
		 printf ("error in zmq_msg_data(), previous %d and current %d counter, data corrupt!\n", msg_counter, new_msg_counter);
		 }

		msg_counter = *(int*) zmq_msg_data(&msg);
		msg_copy = new (zmq_msg_t);
		zmq_msg_init_size(msg_copy, zmq_msg_size(&msg));
		zmq_msg_move(msg_copy, &msg);
		/* wait for every nth thread to limit number of threads and avoid memory leaks */
		if (file_writer_thread[i % no_threads] != 0) {
			pthread_join(file_writer_thread[i % no_threads], NULL);
		}
		/* create a thread which writes the data in a file */
		pthread_create(&(file_writer_thread[i % no_threads]), NULL, file_writer,
				msg_copy);
		thread_ctr++;
		if (i > 0 && i % 1000 == 0) {
			elapsed = zmq_stopwatch_stop(watch_1000);
			throughput = (unsigned long) ((double) 1000 / (double) elapsed
					* 1000000);
			megabytes = (double) (throughput * message_size) / 1000000;
			megabits = (double) (throughput * message_size * 8) / 1000000;

			printf("%d [msg/s]  %.3f [MB/s] %.3f [Mb/s]\n", (int) throughput,
					(double) megabytes, (double) megabits);
			fflush(stdout);
			watch_1000 = zmq_stopwatch_start();
		}

	}

	elapsed = zmq_stopwatch_stop(watch);
	if (elapsed == 0)
		elapsed = 1;

	rc = zmq_msg_close(&msg);
	if (rc != 0) {
		printf("error in zmq_msg_close: %s\n", zmq_strerror(errno));
		return -1;
	}

	throughput = (unsigned long) ((double) message_count / (double) elapsed
			* 1000000);
	megabytes = (double) (throughput * message_size) / 1000000;
	megabits = (double) (throughput * message_size * 8) / 1000000;

	printf("message size: %d [B]\n", (int) message_size);
	printf("message count: %d\n", (int) message_count);
	printf("mean throughput: %d [msg/s]\n", (int) throughput);
	printf("mean throughput for 1000 msg: %.3f [MB/s] %.3f [Mb/s]\n",
			(double) megabytes, (double) megabits);

	rc = zmq_close(s);
	if (rc != 0) {
		printf("error in zmq_close: %s\n", zmq_strerror(errno));
		return -1;
	}

	rc = zmq_term(ctx);
	if (rc != 0) {
		printf("error in zmq_term: %s\n", zmq_strerror(errno));
		return -1;
	}

	return 0;
}
