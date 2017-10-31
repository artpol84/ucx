/*
* Copyright (C) Mellanox Technologies Ltd. 2001-2014.  ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifndef UCS_SPINLOCK_H
#define UCS_SPINLOCK_H

#include <ucs/type/status.h>
#include <pthread.h>


/**
 * Reentrant spinlock.
 */
typedef struct ucs_spinlock {
    pthread_spinlock_t lock;
    int                count;
    pthread_t          owner;
} ucs_spinlock_t;


static inline ucs_status_t ucs_spinlock_init(ucs_spinlock_t *lock)
{
    int ret;

    ret = pthread_spin_init(&lock->lock, 0);
    if (ret != 0) {
        return UCS_ERR_IO_ERROR;
    }

    lock->count = 0;
    lock->owner = 0xfffffffful;
    return UCS_OK;
}

static inline ucs_status_t ucs_spinlock_destroy(ucs_spinlock_t *lock)
{
    int ret;

    ret = pthread_spin_destroy(&lock->lock);
    if (ret != 0) {
        return UCS_ERR_IO_ERROR;
    }

    return UCS_OK;
}

static inline int ucs_spin_is_owner(ucs_spinlock_t *lock, pthread_t self)
{
    return lock->owner == self;
}

#include <unistd.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */

#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#define GET_TS1() ({                             \
	struct timeval tv;                     \
	double ret = 0;                         \
	gettimeofday(&tv, NULL);    \
	ret = tv.tv_sec + 1E-6*tv.tv_usec;      \
	ret;                                    \
	})

extern struct myrecord {
    int tid;
    double ts;
    const char *pref;
    char *fname;
    int line;
    const char *func;
} mytable[10*1024];
extern int mytable_cnt;


static inline void ucs_spin_lock(ucs_spinlock_t *lock)
{
    pthread_t self = pthread_self();

    if (ucs_spin_is_owner(lock, self)) {
        ++lock->count;
        return;
    }

    pthread_spin_lock(&lock->lock);

    lock->owner = self;
    ++lock->count;
}

static inline void ucs_spin_lock_x(ucs_spinlock_t *lock, char *file, int line, const char* func)
{
    pid_t tid = (pid_t)syscall(SYS_gettid);
    pthread_t self = pthread_self();

    if (ucs_spin_is_owner(lock, self)) {
        ++lock->count;

{
    int i = mytable_cnt++;
    mytable[i].pref = "re";
    mytable[i].tid = tid;
    mytable[i].ts = GET_TS1();
    mytable[i].fname = file;
    mytable[i].line = line;
    mytable[i].func = func;
}

        return;
    }

    pthread_spin_lock(&lock->lock);

{
    int i = mytable_cnt++;
    mytable[i].pref = "";
    mytable[i].tid = tid;
    mytable[i].ts = GET_TS1();
    mytable[i].fname = file;
    mytable[i].line = line;
    mytable[i].func = func;
}

    lock->owner = self;
    ++lock->count;
}



static inline void ucs_spin_lock_1(ucs_spinlock_t *lock, FILE *fp)
{
    pid_t tid = (pid_t)syscall(SYS_gettid);
    pthread_t self = pthread_self();
    
    
    fprintf(fp,"ucs_spin_lock_1[%d]: start %lf\n",  tid, GET_TS1());

    if (ucs_spin_is_owner(lock, self)) {
        ++lock->count;
        fprintf(fp,"ucs_spin_lock_1: owner %lf\n",  GET_TS1());
        fprintf(fp,"ucs_spin_lock_1[%d]: owner %lf\n", tid, GET_TS1());
        return;
    }

    fprintf(fp,"ucs_spin_lock_1[%d]: not owner %lf\n", tid, GET_TS1());

    pthread_spin_lock(&lock->lock);
    fprintf(fp,"ucs_spin_lock_1[%d]: spin_lock %lf\n",  tid, GET_TS1());

    if( mytable_cnt ){
	int i;
	fprintf(fp,"ucs_spin_lock:hist\n");

	for(i = 0; i < mytable_cnt; i++) {
	    fprintf(fp,"ucs_spin_%slock[%d]: %lf (%s:%d:%s)\n",
		mytable[i].pref,
		mytable[i].tid,
		mytable[i].ts,
		mytable[i].fname,
		mytable[i].line,
		mytable[i].func);
	}
	mytable_cnt = 0;
    }

    lock->owner = self;
    ++lock->count;
    fprintf(fp,"ucs_spin_lock_1[%d]: done %lf\n",  tid, GET_TS1());
}

static inline int ucs_spin_trylock(ucs_spinlock_t *lock)
{
    pthread_t self = pthread_self();

    if (ucs_spin_is_owner(lock, self)) {
        ++lock->count;
        return 1;
    }

    if (pthread_spin_trylock(&lock->lock) != 0) {
        return 0;
    }

    lock->owner = self;
    ++lock->count;

    return 1;
}

static inline int ucs_spin_trylock_x(ucs_spinlock_t *lock, char *file, int line, const char* func)
{
    pid_t tid = (pid_t)syscall(SYS_gettid);
    pthread_t self = pthread_self();

    if (ucs_spin_is_owner(lock, self)) {
        ++lock->count;
        return 1;
    }

    if (pthread_spin_trylock(&lock->lock) != 0) {
        return 0;
    }

{
    int i = mytable_cnt++;
    mytable[i].pref = "try";
    mytable[i].tid = tid;
    mytable[i].ts = GET_TS1();
    mytable[i].fname = file;
    mytable[i].line = line;
    mytable[i].func = func;
}

    lock->owner = self;
    ++lock->count;

    return 1;
}


static inline void ucs_spin_unlock(ucs_spinlock_t *lock)
{
    --lock->count;
    if (lock->count == 0) {
        lock->owner = 0xfffffffful;
        pthread_spin_unlock(&lock->lock);
    }
}

static inline void ucs_spin_unlock_x(ucs_spinlock_t *lock, char *file, int line, const char* func)
{

    pid_t tid = (pid_t)syscall(SYS_gettid);


    --lock->count;
    if (lock->count == 0) {
        lock->owner = 0xfffffffful;

{
    int i = mytable_cnt++;
    mytable[i].pref = "release-un";
    mytable[i].tid = tid;
    mytable[i].ts = GET_TS1();
    mytable[i].fname = file;
    mytable[i].line = line;
    mytable[i].func = func;
}

        pthread_spin_unlock(&lock->lock);
    } else
    {
    int i = mytable_cnt++;
    mytable[i].pref = "un";
    mytable[i].tid = tid;
    mytable[i].ts = GET_TS1();
    mytable[i].fname = file;
    mytable[i].line = line;
    mytable[i].func = func;
}


}

static inline void ucs_spin_unlock_1(ucs_spinlock_t *lock, FILE *fp)
{

    int tid = (pid_t)syscall(SYS_gettid);

    fprintf(fp,"ucs_spin_unlock_1[%d]: start %lf\n",  tid, GET_TS1());

    --lock->count;
    if (lock->count == 0) {
        lock->owner = 0xfffffffful;
        pthread_spin_unlock(&lock->lock);
        fprintf(fp,"ucs_spin_unlock_1[%d]: release %lf\n",  tid, GET_TS1());
    }
    fprintf(fp,"ucs_spin_unlock_1[%d]: done %lf\n",  tid, GET_TS1());
}

#endif
