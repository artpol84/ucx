/**
 * Copyright (C) Mellanox Technologies Ltd. 2018.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include "spinlock.h"

#include <ucs/debug/log.h>
#include <string.h>
#include "ucs/arch/cpu.h"
#include <unistd.h>

volatile int32_t lock_profiles_count = 0;
volatile __thread int32_t lock_profile_index_loc = -1;
locking_profile_t lock_profiles[1024] = { { { 0 } } };

static int _get_rank()
{
    static int rank = -1;
    if( rank < 0 ) {
        char *ptr = getenv("PMIX_RANK");
        rank = atoi(ptr);
    }
    return rank;
}

static char *_get_jobid()
{
    static char jobid[256] = "";
    if( strlen(jobid) == 0 ) {
        char *s_stepid = getenv("SLURM_STEPID");
        char *s_jobid = getenv("SLURM_JOBID");
        if(s_stepid == NULL || s_jobid == NULL) {
            sprintf(jobid, "0.0");
        } else {
            sprintf(jobid, "%s.%s", s_jobid, s_stepid);
        }
    }
    return jobid;
}


static void _print_prof_metric(FILE *fp, locking_metrics_t *metric,char *prefix)
{
    int avg_divider = 0;
#if (UCX_SPLK_PROF_WAIT_TS)
    avg_divider = metric->spinned;
#elif (UCX_SPLK_PROF_FASTP_TS)
    avg_divider = metric->invoked;
#endif
    (void)avg_divider;
    if(!metric->invoked) {
        return;
    }
    fprintf(fp,"\t%s\n", prefix);
    fprintf(fp, "\t\tcount:\t%lu\n", metric->invoked);
    fprintf(fp, "\t\twaited:\t%lu\n", metric->spinned);

    fprintf(fp, "\t\tspins:\ttot=%lu, max=%lu, avg=%lf\n",
            metric->spins,
            metric->spins_max,
            (double)metric->spins / metric->spinned);

#if (UCX_SPLK_PROF_WAIT_TS)
    char *ts_prefix="W";
#elif (UCX_SPLK_PROF_FASTP_TS)
    char *ts_prefix="FP";
#endif
#if (UCX_SPLK_PROF_WAIT_TS || UCX_SPLK_PROF_FASTP_TS)
    fprintf(fp, "\t\t%s-cyc:\ttot=%lucyc (%lfs), max=%lucyc (%lfus), "
            "avg=%lfcyc (%lfus)\n", ts_prefix,
            metric->cycles,
            (double)metric->cycles / ucs_arch_get_clocks_per_sec(),
            metric->cycles_max,
            1E6 * (double)metric->cycles_max / ucs_arch_get_clocks_per_sec(),
            (double)metric->cycles / avg_divider,
            (double)metric->cycles / avg_divider / ucs_arch_get_clocks_per_sec() * 1E6);
#endif
}

static void _print_profile(FILE *fp, locking_profile_t *profile)
{
    _print_prof_metric(fp, &profile->cum, "CUMULATIVE!");

    _print_prof_metric(fp, &profile->diff[SPINLOCK_NONE][SPINLOCK_POST],
                       "NONE-POST");
    _print_prof_metric(fp, &profile->diff[SPINLOCK_NONE][SPINLOCK_PROGRESS],
                       "NONE-PROGRESS");
    _print_prof_metric(fp, &profile->diff[SPINLOCK_NONE][SPINLOCK_RELEASE],
                       "NONE-RELEASE");


    _print_prof_metric(fp, &profile->diff[SPINLOCK_RELEASE][SPINLOCK_RELEASE],
                       "RELEASE-RELEASE");
    _print_prof_metric(fp, &profile->diff[SPINLOCK_RELEASE][SPINLOCK_POST],
                       "RELEASE-POST");
    _print_prof_metric(fp, &profile->diff[SPINLOCK_RELEASE][SPINLOCK_PROGRESS],
                       "RELEASE-PROGRESS");


    _print_prof_metric(fp, &profile->diff[SPINLOCK_POST][SPINLOCK_RELEASE],
                       "POST-RELEASE");
    _print_prof_metric(fp, &profile->diff[SPINLOCK_POST][SPINLOCK_POST],
                       "POST-POST");
    _print_prof_metric(fp, &profile->diff[SPINLOCK_POST][SPINLOCK_PROGRESS],
                       "POST-PROGRESS");

    _print_prof_metric(fp, &profile->diff[SPINLOCK_PROGRESS][SPINLOCK_RELEASE],
                       "PROGRESS-RELEASE");
    _print_prof_metric(fp, &profile->diff[SPINLOCK_PROGRESS][SPINLOCK_POST],
                       "PROGRESS-POST");
    _print_prof_metric(fp, &profile->diff[SPINLOCK_PROGRESS][SPINLOCK_PROGRESS],
                       "PROGRESS-PROGRESS");

    _print_prof_metric(fp, &profile->diff[SPINLOCK_ASYNC][SPINLOCK_POST],
                       "ASYNC-POST");
    _print_prof_metric(fp, &profile->diff[SPINLOCK_ASYNC][SPINLOCK_PROGRESS],
                       "ASYNC-PROGRESS");
}

static void _merge_metrics(locking_metrics_t *dst, locking_metrics_t *src)
{
    dst->invoked += src->invoked;
    dst->spinned += src->spinned;
    dst->spins += src->spins;
    if( dst->spins_max < src->spins_max) {
        dst->spins_max = src->spins_max;
    }
    dst->cycles += src->cycles;
    if( dst->cycles_max < src->cycles_max) {
        dst->cycles_max = src->cycles_max;
    }
}

void ucx_lock_dbg_report()
{
    locking_profile_t profile = { { 0 } };
    int i;
    for(i=0; i<lock_profiles_count; i++) {
        int j,k;
        _merge_metrics(&profile.cum, &lock_profiles[i].cum);
        for(j=0; j<SPINLOCK_OP_CNT; j++) {
            for(k=0; k<SPINLOCK_OP_CNT; k++) {
                _merge_metrics(&profile.diff[j][k], &lock_profiles[i].diff[j][k]);
            }
        }
    }

    char *ptr = getenv("UCX_LOCK_PROFILE_PATH");
    if(NULL == ptr) {
        // No profiling collection was requested
        return;
    }

    char path[1024], hname[256] = "", *hname_dot = NULL;
    gethostname(hname, 256);
    hname_dot = strchr(hname,'.');
    if( hname_dot ){
        *hname_dot = '\0';
    }

#if (UCX_SPLK_PROF_WAIT_TS)
    char *ts_prefix="ts_spin_wait";
#elif (UCX_SPLK_PROF_FASTP_TS)
    char *ts_prefix="ts_fast_path";
#else
    char *ts_prefix="spin_count";
#endif
    sprintf(path, "%s/prof_%s_j-%s_%s.%d",
            ptr, ts_prefix, _get_jobid(), hname, _get_rank());
    FILE *fp = fopen(path, "w");
    if( NULL == fp) {
        ucs_error("Cannot open \"%s\" for writing\n", ptr);
        return;
    }

    fprintf(fp, "Cumulative info:\n");
    _print_profile(fp, &profile);

    fprintf(fp, "Per-thread info:\n");
    for(i=0; i < lock_profiles_count; i++) {
        fprintf(fp, "Thread #%d:\n", i);
        _print_profile(fp, &lock_profiles[i]);
    }
    fclose(fp);
}

ucs_status_t ucs_spinlock_init(ucs_spinlock_t *lock)
{
    int ret;

    ret = pthread_spin_init(&lock->lock, 0);
    if (ret != 0) {
        return UCS_ERR_IO_ERROR;
    }

    lock->count = 0;
    lock->owner = 0xfffffffful;
    lock->is_profiled = 0;
    return UCS_OK;
}

ucs_status_t ucs_spinlock_init_prof(ucs_spinlock_t *lock)
{
    ucs_spinlock_init(lock);
    lock->is_profiled = 1;
    return UCS_OK;
}

void ucs_spinlock_destroy(ucs_spinlock_t *lock)
{
    int ret;

    if( lock->is_profiled) {
        ucx_lock_dbg_report();
    }
    if (lock->count != 0) {
        ucs_warn("destroying spinlock %p with use count %d (owner: 0x%lx)",
                 lock, lock->count, lock->owner);
    }

    ret = pthread_spin_destroy(&lock->lock);
    if (ret != 0) {
        ucs_warn("failed to destroy spinlock %p: %s", lock, strerror(ret));
    }
}
