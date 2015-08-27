/* Copyright (c) 2010-2015 Bo Lin
 * Copyright (c) 2010-2015 Yanhong Annie Liu
 * Copyright (c) 2010-2015 Stony Brook University
 * Copyright (c) 2010-2015 The Research Foundation of SUNY
 * 
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#define _XOPEN_SOURCE 500

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/resource.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <time.h>

#define SERVER_PORT_MIN 19999
#define SERVER_PORT_MAX 29999
#define MAX_NPEERS_LIMIT 500    /* Safety guard: max number of processes */
#define MAX_RETRY 10            /* Number of times to try sending a packet
                                 * before giving up */

/* Should be enums... */
#define REQUEST 1
#define RELEASE 2
#define ACK 3
#define DONE 4
#define START 5

#define BRDCAST_ADDR -1
#define SERVER_ADDR -2

#define BACKLOG 25
#define TRUE 1
#define FALSE 0

#define MAX(x, y) (((x) > (y)) ? (x) : (y))

typedef char byte;
typedef struct _peer {
    int id;
    long clock;
    byte req;
} peer_t;
typedef int (* await_cond_fun)(void * arg);

typedef struct _packet {
    int type;
    int dest;
    int src;

    union {
        struct {
            struct timeval usrtime;
            struct timeval systime;
            long maxrss;
        };
        long clock;
    };
} packet_t;

/* All global variables are grouped into this structure. Such a structure
 * could be used in a simulation, with one copy for each peer: */
struct __global {
    /* ----- Common states ----- */
    int id;                     /* Our peer id */
    int npeers;                 /* Total number of peers in network. Range of
                                 * valid peer id values is thus [0..npeers-1] */
    int num_done;               /* Number of nodes that are done */
    int sockfd;                 /* The socket we listen on */
    long server_port;            /* The port number server bound to */
    int rounds_to_run;          /* Number of rounds to run */

    union {
        /* ----- Client states ----- */
        struct {
            long clock;                 /* Our logical clock */
            peer_t* peer_set;   /* An array of 'peer_t's, one for each peer,
                                 * including self. */
            int *acks;          /* Array indicating which peers have ACKed our
                                 * REQUEST */
            int ack_count;      /* Number of distinct ACKs we have recieved
                                 * for our REQUEST */
            struct sockaddr_in local_addr; /* Saves the sockaddr_in structure
                                            * for the interface we bound to
                                            * (for use by sendto). */
            struct rusage rudata_start;
            int started;
        };

        /* ----- Server states ----- */
        struct {
            int *fds;
            pid_t *children;
            struct timeval total_utime;
            struct timeval total_stime;
            long total_memory;
        };
    };

} g;

static int recvpkt(int sockfd, packet_t *packet, int block);
static void handle_message(packet_t *data);
static void client_connect();

/* ********** Auxiliary functions **********  */

void tv_add(struct timeval *tva, struct timeval *tvb)
{
    tva->tv_sec += tvb->tv_sec;
    tva->tv_usec += tvb->tv_usec;
    if (tva->tv_usec > 1000000) {
        tva->tv_usec %= 1000000;
        tva->tv_sec ++;
    }
}

void tv_sub(struct timeval *tva, struct timeval *tvb)
{
    tva->tv_sec -= tvb->tv_sec;
    tva->tv_usec -= tvb->tv_usec;
    if (tva->tv_usec < 0) {
        tva->tv_usec += 1000000;
        tva->tv_sec --;
    }
}

int64_t timespecDiff(struct timespec *timeA_p, struct timespec *timeB_p)
{
  return ((timeA_p->tv_sec * 1000000000) + timeA_p->tv_nsec) -
           ((timeB_p->tv_sec * 1000000000) + timeB_p->tv_nsec);
}

static int isserver()
{
    return (g.id == -1);
}

void die(const char *s, int code)
{
    fputs(s, stderr);
    fflush(stderr);
    if (isserver()) {
        if (g.children) {
            for (pid_t *p = g.children; p < g.children + g.npeers; p++)
                if (*p)
                    kill(*p, SIGTERM);
        }
    }
    exit(code);
}

static void sighandler(int signum)
{
    if (signum == SIGCHLD)
        die("Child terminated unexpectedly.", 30);
}

void procrastinate()
{
    struct timeval tv;
    tv.tv_sec = (long) 3;
    tv.tv_usec = 0;
    if (select(FD_SETSIZE, NULL, NULL, NULL, &tv) < 0)
        die("select\n", 10);
}

void yield(int block)
{
    /* Called by the main thread to indicate willingness to relinquish
     * control. Corresponds to a "label". If block is NULL, yield() will
     * return immediately if there is no work to be done (no pending
     * packets). Otherwise, yield() will block until the next packet
     * arrives. */
    int flag = 0, r;
    packet_t pack;

l:
    r = recvpkt(g.sockfd, &pack, block);
    if (r > 0)
        handle_message(&pack);
    else if (r < 0) {
        /* Retry connection */
        close(g.sockfd);
        client_connect();
        goto l;
    }
}

void await(await_cond_fun test)
{
    while (!test(NULL))
        yield(1);
}

static int peercmp(peer_t *p1, peer_t *p2)
{
    return (p1->clock < p2->clock ||
            (p1->clock == p2->clock && p1->id < p2->id));
}

static peer_t *minpeer()
{
    peer_t *result = NULL;
    for (int i = 0; i < g.npeers; i++) {
        if (g.peer_set[i].req &&
            (!result || peercmp((g.peer_set + i), result)))
            result = g.peer_set + i;
    }
    return result;
}

double get_wallclock_sample()
{
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC_RAW, &tp);
    return (double)tp.tv_sec + (double)tp.tv_nsec / 1e9;
}

static void dump_perf_stats(double wallclock)
{
    /* Prints performance statistics in JSON format */
    tv_add(&g.total_stime, &g.total_utime); /* Use the systime variable to
                                             * store the total process
                                             * time */
    printf(
        "###OUTPUT: {\"Total_memory\": %ld, \
\"Wallclock_time\": %f, \"Total_processes\": %d, \
\"Total_process_time\": %ld.%06ld, \
\"Total_user_time\": %ld.%06ld}\n",
        g.total_memory, wallclock, g.npeers,
        g.total_stime.tv_sec, (long) g.total_stime.tv_usec,
        g.total_utime.tv_sec, (long) g.total_utime.tv_usec);
}

/* ********** Network support functions ********** */

static void send_message(int fd, const packet_t *data)
{
    ssize_t sentlen = 0, nleft = sizeof(packet_t);
    char *ptr = (char *) data;

    while (nleft > 0) {
        if ((sentlen = send(fd, ptr, nleft, 0)) < 0)
            die("send error.\n", 21);

        nleft -= sentlen;
        ptr += sentlen;
    }
}

static int recvpkt(int sockfd, packet_t *packet, int block)
{
    ssize_t rcvlen = 0, nleft = sizeof(packet_t);
    int flag = 0;
    char *ptr = (char *) packet;

    if (!block)
        flag = MSG_DONTWAIT;
    while (nleft > 0) {
        if ((rcvlen = recv(sockfd, ptr, nleft, flag)) < 0) {
            int errv = errno;
            if (errv == EWOULDBLOCK || errv == EAGAIN) /* No pending packets */
                return 0;
            else if (errv == 104)
                return -1;
            else {
                printf("%s:%d", strerror(errv), errv);
                die("recv error\n", 4);
            }
        }
        nleft -= rcvlen;
        ptr += rcvlen;
    }
    return 1;
}

static void send_reply(int peer, int type)
{
    /* Send message "type" to "peer" */
    packet_t packet;

    packet.dest = peer;
    packet.src = g.id;
    packet.type = type;
    packet.clock = g.clock;

    send_message(g.sockfd, &packet);
}

static void broadcast(int type)
{
    /* Broadcast message to everyone (including self) */
    packet_t pkt;
    int i;

    pkt.type = type;
    pkt.dest = BRDCAST_ADDR;
    pkt.src = g.id;
    pkt.clock = g.clock;

    if (isserver())
        for (i = 0; i < g.npeers; i++)
            send_message(g.fds[i], &pkt);
    else
        send_message(g.sockfd, &pkt);
}

static void client_connect()
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    packet_t pack;

    printf("%d unfrozen.\n", g.id);
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM; /* TCP socket */

    if (getaddrinfo("localhost", NULL, &hints, &result) != 0)
        die("getaddrinfo.\n", 8);

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        ((struct sockaddr_in *)rp->ai_addr)->sin_port = htons(g.server_port);
        if (connect(g.sockfd, rp->ai_addr, rp->ai_addrlen) == 0) {
            g.local_addr = *((struct sockaddr_in *)rp->ai_addr);
            break;                  /* Success */
        }
    }

    if (rp == NULL)
        die("connect failed.\n", 9);

    freeaddrinfo(result);

    pack.dest = SERVER_ADDR;
    pack.src = g.id;
    send_message(g.sockfd, &pack);
}

static void bind_server_socket() {
    struct addrinfo hints;
    struct addrinfo *result, *rp;

    if ((g.sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        die("socket error", 2);

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM; /* TCP socket */

    if (getaddrinfo("localhost", NULL, &hints, &result) != 0)
        die("getaddrinfo.\n", 8);

    srand(time(NULL));
    for (int i = 0 ; i < MAX_RETRY; i ++) {
        g.server_port = (SERVER_PORT_MIN +
                         rand() % (SERVER_PORT_MAX - SERVER_PORT_MIN));
        for (rp = result; rp != NULL; rp = rp->ai_next) {
            ((struct sockaddr_in *)rp->ai_addr)->sin_port = htons(g.server_port);
            if (bind(g.sockfd, rp->ai_addr, rp->ai_addrlen) == 0) {
                break;                  /* Success */
            }
        }
        if (rp != NULL)
            break;
    }

    if (rp == NULL)
        die("Bind failed.\n", 9);

    freeaddrinfo(result);
}

/* ********** Startup and shutdown routines ********** */

static void unfreeze_child_procs() {
    for (pid_t *p = g.children; p < g.children + g.npeers; p++) {
        kill(*p, SIGUSR1);
    }
}

static void connect_peers()
{
    /* Binds sockfd to "localhost" on port, saves sockaddr_in struct bound
     * to in addr. */
    int connected = 0;
    if (listen(g.sockfd, BACKLOG) < 0)
        die("Listen error.\n", 10);

    unfreeze_child_procs();
    while (connected < g.npeers) {
        int fd;
        packet_t pack;

        fd = accept(g.sockfd, NULL, NULL);
        if (fd == -1) {
            fprintf(stderr, "Accept error.\n");
            continue;
        }
        recvpkt(fd, &pack, 1);
        if (pack.src < 0 || pack.src >= g.npeers)
            die("Invalid src.\n", 20);
        g.fds[pack.src] = fd;
        connected ++;
    }
    printf("All peers connected.\n");
}

static void shutdown_network()
{
    for (int *i = g.fds; i < g.fds + g.npeers; i++)
        close(*i);
    close(g.sockfd);
}

static void join_children()
{
    int result;
    if (g.children)
        for (pid_t *p = g.children; p < g.children + g.npeers; p ++)
            /* We don't care about the child return status at this point */
            waitpid(*p, &result, 0);
}

static void setup(int npeers, int rounds)
{
    sigset_t mask;
    sigset_t orig_mask;
    g.npeers = npeers;
    g.id = -1;
    g.rounds_to_run = rounds;
    g.num_done = 0;
    g.started = 0;

    sigemptyset(&mask);
    sigaddset(&mask, SIGUSR1);
    if (sigprocmask(SIG_BLOCK, &mask, &orig_mask) < 0)
        die("Unable to mask USR1.", 13);
    if (signal(SIGUSR1, sighandler) == SIG_ERR)
        die("Unable to install USR1 handler.", 11);
    if (signal(SIGCHLD, sighandler) == SIG_ERR)
        die("Unable to install CHLD handler.", 11);
    /* Server */
    g.fds = (int *) malloc(sizeof(int) * npeers);
    if (!g.fds)
        die("Out of memory.\n", 1);
    memset((void *) g.fds, 0, sizeof(int) * npeers);
    g.children = (pid_t *) malloc(sizeof(pid_t) * npeers);
    if (!g.children)
        die("Out of memory.\n", 2);
    memset((void *) g.children, 0, sizeof(pid_t) * npeers);
    memset((void *) &g.total_utime, 0, sizeof(struct timeval));
    memset((void *) &g.total_stime, 0, sizeof(struct timeval));

    g.total_memory = 0;
    bind_server_socket();

    for (int i = 0; i < npeers; i++) {
        printf("Forking child %d.\n", i);
        fflush(stdout);
        g.children[i] = fork();
        if (g.children[i] < 0) {
            die("Forking error.\n", 20);
        }
        else if (g.children[i] == 0) {
            /* is client */
            g.id = i;
            break;
        }
        else if (i == npeers-1) {
            return;
        }
    }

    /* Client */
    free(g.children);
    free(g.fds);
    g.peer_set = (peer_t *) malloc (sizeof(peer_t) * npeers);
    g.acks = (int *) malloc( sizeof(int) * npeers);
    if (!g.peer_set || !g.acks)
        die("Out of memory.\n", 1);
    memset((void *) g.peer_set, 0, sizeof(peer_t) * npeers);
    memset((void *) g.acks, 0, sizeof(int) * npeers);
    memset((void *) &g.rudata_start, 0, sizeof(struct rusage));

    for (int i = 0; i < npeers; i++) {
        g.peer_set[i].id = i;
        g.peer_set[i].clock = 0;
        g.peer_set[i].req = 0;
    }
    g.ack_count = 0;
    g.clock = 0;
    g.started = 0;
    sigsuspend(&orig_mask);          /* Give the server time to finish setup */

    if ((g.sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        die("socket error", 2);
}

static void send_stats()
{
    packet_t packet;
    struct rusage rudata_end;

    getrusage(RUSAGE_SELF, &rudata_end);
    tv_sub(&rudata_end.ru_utime, &g.rudata_start.ru_utime);
    tv_sub(&rudata_end.ru_stime, &g.rudata_start.ru_stime);
    packet.dest = SERVER_ADDR;
    packet.src = g.id;
    packet.type = DONE;
    packet.usrtime = rudata_end.ru_utime;
    packet.systime = rudata_end.ru_stime;
    packet.maxrss = rudata_end.ru_maxrss;
    send_message(g.sockfd, &packet);
}

static int server_done_cond(void *arg)
{
    return (g.num_done == g.npeers);
}

/* ********** Main algorithm ********** */

static int enter_cs_cond(void *arg)
{
    peer_t *min = minpeer();
    return ((g.ack_count == g.npeers) && min && (min->id == g.id));
}

static int start_cond(void *arg)
{
    return g.started;
}

void enter_critical_section()
{
    memset((void *) g.acks, 0, sizeof(int) * g.npeers);
    g.ack_count = 0;
    broadcast(REQUEST);

    await(enter_cs_cond);
}

void leave_critical_section()
{
    broadcast(RELEASE);
}

void site()
{
    int count = 0;
    struct timespec start, end;
    uint64_t timeelapsed;

    await(start_cond);
    getrusage(RUSAGE_SELF, &g.rudata_start);
    while (count < g.rounds_to_run)
    {
        /* Non-critical section: */
//        procrastinate();
        /* These are "labels": */
        yield(0);

        clock_gettime(CLOCK_MONOTONIC_RAW, &start);
        /* Try entering CS: */
        enter_critical_section();
        clock_gettime(CLOCK_MONOTONIC_RAW, &end);
        timeelapsed = timespecDiff(&end, &start);
//        printf("Time to enter: %llu\n", timeelapsed);

        /* In CRITICAL-SECTION! */
        printf("P%d is in CS with clock %ld.\n", g.id, g.clock);
        yield(0);
        //procrastinate();
        yield(0);
        /* Release ownership of CS */
        printf("P%d is leaving CS - %d.\n", g.id, count);
        leave_critical_section();

        fflush(stdout);
        count++;
    }

    send_stats();
    await(server_done_cond);
}

static void handle_message(packet_t *data)
{
    /* Process one packet */
    int from = data->src;
    if (from < -1 || from >= g.npeers) { /* Not a valid peer */
        printf("Invalid peer %d, dropping packet.\n", from);
        return;
    }

    switch (data->type) {
    case REQUEST:
        g.peer_set[from].req = 1;
        g.peer_set[from].clock = data->clock;
        g.clock = MAX(g.clock, data->clock) + 1;
        send_reply(from, ACK);
        break;

    case ACK:
        if (!g.acks[from]) {
            g.ack_count++;
            g.acks[from] = 1;
        }
        break;

    case RELEASE:
        g.peer_set[from].req = 0;
        break;

    case START:
        g.started = 1;
        break;

    case DONE:
        if (isserver()) {
            g.num_done++;
            tv_add(&g.total_utime, &data->usrtime);
            tv_add(&g.total_stime, &data->systime);
            g.total_memory += data->maxrss;
        }
        else {
            g.num_done = g.npeers;
        }

    default:
        break;
    }
}

void server_message_loop()
{
    fd_set rfds;
    int i, r, to, nfds;
    packet_t pack;

    while (g.num_done < g.npeers) {
        FD_ZERO(&rfds);
        nfds = 0;
        for (i = 0; i < g.npeers; i++) {
            FD_SET(g.fds[i], &rfds);
            nfds = MAX(nfds, g.fds[i]);
        }

        if ((r = select(nfds+1, &rfds, NULL, NULL, NULL)) < 0)
            die("select\n", 12);

        for (i = 0; i < g.npeers; i++) {
            if (FD_ISSET(g.fds[i], &rfds)) {

                recvpkt(g.fds[i], &pack, 1);
                to = pack.dest;
                if (to == BRDCAST_ADDR) {
                    for (i = 0; i < g.npeers; i++)
                        send_message(g.fds[i], &pack);
                } else if (to == SERVER_ADDR) {
                    handle_message(&pack);
                } else {
                    send_message(g.fds[to], &pack);
                }
            }
        }
    }
}

/* ********** Program entry point ********** */

int main(int argc, char *argv[])
{
    if (argc > 3) {
        fprintf(stderr, "Usage: %s npeers rounds.\n", argv[0]);
        fflush(stderr);
        exit(1);
    }
    int npeers = 10;
    int nrounds = 5;
    if (argc > 1) {
        npeers = atoi(argv[1]);
    }
    if (argc > 2) {
        nrounds = atoi(argv[2]);
    }
    if (npeers < 1 || npeers > MAX_NPEERS_LIMIT) {
        die("Woah, too few or too many processes!", 10);
    }

    setup(npeers, nrounds);
    if (isserver()) {
        double wallclock_start, wallclock_total;
        connect_peers();
        wallclock_start = get_wallclock_sample();
        broadcast(START);
        server_message_loop();
        signal(SIGCHLD, SIG_DFL);
        broadcast(DONE);
        wallclock_total = get_wallclock_sample() - wallclock_start;
        shutdown_network();
        join_children();
        dump_perf_stats(wallclock_total);
    }
    else {
        client_connect();
        site();
        close(g.sockfd);
    }
    return 0;
}
