/*
* Copyright (C) Mellanox Technologies Ltd. 2001-2014.  ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifndef UCS_SPINLOCK_H
#define UCS_SPINLOCK_H

#include <ucs/type/status.h>
#include <pthread.h>
#include <stdint.h>
#include <ucs/arch/atomic.h>

BEGIN_C_DECLS

/** @file spinlock.h */

typedef enum {
    SPINLOCK_NONE,
    SPINLOCK_POST,
    SPINLOCK_PROGRESS,
    SPINLOCK_ASYNC,
    SPINLOCK_OP_CNT
} spinlock_operation_t;

/**
 * Reentrant spinlock.
 */
typedef struct ucs_spinlock {
    pthread_spinlock_t lock;
    int                count;
    pthread_t          owner;
    spinlock_operation_t op_type;
    int is_profiled;
} ucs_spinlock_t;


ucs_status_t ucs_spinlock_init(ucs_spinlock_t *lock);
ucs_status_t ucs_spinlock_init_prof(ucs_spinlock_t *lock);

void ucs_spinlock_destroy(ucs_spinlock_t *lock);

static inline int ucs_spin_is_owner(ucs_spinlock_t *lock, pthread_t self)
{
    return lock->owner == self;
}

/* ================ REGULAR LOCKING ==================================*/
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

static inline void ucs_spin_unlock(ucs_spinlock_t *lock)
{
    --lock->count;
    if (lock->count == 0) {
        lock->owner = 0xfffffffful;
        pthread_spin_unlock(&lock->lock);
    }
}

/* ================ DEBUG LOCKING ==================================*/

/* Controls */
//#define UCX_SPLK_PROF_WAIT_TS 1
//#define UCX_SPLK_PROF_FASTP_TS 1

#ifndef UCX_RDTSCP_INSTR
#define UCX_RDTSCP_INSTR "rdtscp"
#endif

typedef struct {
    uint64_t spins;
    uint64_t spins_max;
    uint64_t cycles;
    uint64_t cycles_max;
} locking_metrics_t;

typedef struct {
    uint64_t invoked;
    uint64_t spinned;
    locking_metrics_t cum;
    locking_metrics_t diff[SPINLOCK_OP_CNT][SPINLOCK_OP_CNT];
} locking_profile_t;

extern volatile int32_t lock_profiles_count;
extern volatile __thread int32_t lock_profile_index_loc;
extern locking_profile_t lock_profiles[1024];

static inline locking_profile_t *ucx_lock_dbg_thread_local()
{
    if( 0 > lock_profile_index_loc ) {
        // initialize the profile
        lock_profile_index_loc =
                ucs_atomic_fadd32((volatile uint32_t*)&lock_profiles_count, 1);
    }
    return &lock_profiles[lock_profile_index_loc];
}

static inline void
_spinlock_prof(pthread_spinlock_t *l,
                   uint64_t *ovh_cycles, uint64_t *spin_cnt)
{
    uint64_t cntr = 0;
#if (UCX_SPLK_PROF_WAIT_TS || UCX_SPLK_PROF_FASTP_TS)
    uint64_t ts1, ts2;
#endif

    asm volatile (

        // Initialize the timestamps
#if (UCX_SPLK_PROF_WAIT_TS || UCX_SPLK_PROF_FASTP_TS)
        "    xor %%r10, %%r10\n"
        "    xor %%r11, %%r11\n"
#endif

        // Get the timestamp prior to fastpath attempt
#if UCX_SPLK_PROF_FASTP_TS
        // Get the timestamp prior to lock acquisition attempt
        "    " UCX_RDTSCP_INSTR "\n"
        "    shl $32, %%rdx\n"
        "    or %%rax, %%rdx\n"
        "    mov %%rdx, %%r10\n"
#endif

        // Try to obtain the lock and exit if successful (*lock == 0)
        "    lock decl (%[lock])\n"

        // Get the timestamp after the fastpath attempt
#if (UCX_SPLK_PROF_FASTP_TS)
       // End timestamp of the fastpath code
       "    " UCX_RDTSCP_INSTR "\n"
       "    shl $32, %%rdx\n"
       "    or %%rax, %%rdx\n"
       "    mov %%rdx, %%r11\n"
#endif
        "    je slk_exit_%=\n"

        // Get the timestamp at the beginning of the waiting loop
#if (UCX_SPLK_PROF_WAIT_TS)
        "    " UCX_RDTSCP_INSTR "\n"
        "    shl $32, %%rdx\n"
        "    or %%rax, %%rdx\n"
        "    mov %%rdx, %%r10\n"
#endif

        // reset $rax as we will use it to count spin iterations
        "    xor %%rax, %%rax\n"

        // Jump to the spinning loop
        "    jmp slk_sleep_%=\n"

         // Acquire attempt
        "slk_acquire_%=:\n"
        "    lock decl (%[lock])\n"
        "    jne slk_sleep_%=\n"
        "    jmp slk_exit_%=\n"

        // Spinning loop
        "slk_sleep_%=:\n"
        "    pause\n"
        "    incq %%rax\n"
        "    cmpl   $0x0, (%[lock])\n"
        "    jg     slk_acquire_%=\n"
        "    jmp    slk_sleep_%=\n"

        // Exit sequence
        "slk_exit_%=:\n"

        // Get the timestamp at the end of the waiting loop
#if UCX_SPLK_PROF_WAIT_TS
        "    " UCX_RDTSCP_INSTR "\n"
        "    shl $32, %%rdx\n"
        "    or %%rax, %%rdx\n"
        "    mov %%rdx, %%r11\n"
#endif

        // Store the measured metrics
        "    mov %%rax, (%[cntr])\n"
#if (UCX_SPLK_PROF_FASTP_TS || UCX_SPLK_PROF_WAIT_TS)
        "    mov %%r10, (%[ts1])\n"
        "    mov %%r11, (%[ts2])\n"
#endif
        :
        : [lock] "r" (l), [cntr] "r" (&cntr)
#if (UCX_SPLK_PROF_FASTP_TS || UCX_SPLK_PROF_WAIT_TS)
          , [ts1] "r" (&ts1), [ts2] "r" (&ts2)
#endif
        : "memory", "rax"

#if (UCX_SPLK_PROF_FASTP_TS || UCX_SPLK_PROF_WAIT_TS)
                , "rdx", "r10", "r11"
#endif
        );
    *spin_cnt = cntr;

    *ovh_cycles = 0;
#if UCX_SPLK_PROF_WAIT_TS
    if (ts1 != 0) {
        *ovh_cycles = ts2 - ts1;
    }
#endif
}

static inline void ucs_spin_lock_prof(ucs_spinlock_t *lock, spinlock_operation_t op)
{
    uint64_t cycles, count;
    locking_profile_t *prof = ucx_lock_dbg_thread_local();
    pthread_t self = pthread_self();
    spinlock_operation_t owner_op = SPINLOCK_NONE;

    if (ucs_spin_is_owner(lock, self)) {
        ++lock->count;
        return;
    }
    owner_op = lock->op_type;

    _spinlock_prof(&lock->lock, &cycles, &count);

    lock->owner = op;
    lock->owner = self;
    ++lock->count;

    // Profile part
    prof->invoked++;
    prof->cum.spins += count;
    if( prof->cum.spins_max < count ) {
        prof->cum.spins_max = count;
    }
    prof->cum.cycles += cycles;
    if( prof->cum.cycles_max < count ) {
        prof->cum.cycles_max = count;
    }
    if( count ) {
        // Count number of times the acquisition was delayed because someone
        // else was holding a lock
        locking_metrics_t *metric = &prof->diff[owner_op][op];
        prof->spinned++;
        metric->spins += count;
        if( metric->spins_max < count ) {
            metric->spins_max = count;
        }
        metric->cycles += cycles;
        if( metric->cycles_max < count ) {
            metric->cycles_max = count;
        }

    }
}

static inline int ucs_spin_trylock_prof(ucs_spinlock_t *lock,
                                        spinlock_operation_t op)
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
    lock->op_type = op;
    ++lock->count;
    return 1;
}

static inline void ucs_spin_unlock_prof(ucs_spinlock_t *lock)
{
    --lock->count;
    if (lock->count == 0) {
        lock->owner = 0xfffffffful;
        lock->op_type = SPINLOCK_NONE;
        pthread_spin_unlock(&lock->lock);
    }
}

END_C_DECLS

#endif
