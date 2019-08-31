/**
 * Copyright (C) Mellanox Technologies Ltd. 2001-2015.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include "eager.h"
#include "rndv.h"
#include "tag_match.inl"
#include "offload.h"

#include <ucp/core/ucp_worker.h>
#include <ucp/core/ucp_request.inl>
#include <ucs/datastruct/mpool.inl>
#include <ucs/datastruct/queue.h>


static UCS_F_ALWAYS_INLINE void
ucp_tag_recv_request_completed(ucp_request_t *req, ucs_status_t status,
                               ucp_tag_recv_info_t *info, const char *function)
{
    ucs_trace_req("%s returning completed request %p (%p) stag 0x%"PRIx64" len %zu, %s",
                  function, req, req + 1, info->sender_tag, info->length,
                  ucs_status_string(status));

    req->status = status;
    if ((req->flags |= UCP_REQUEST_FLAG_COMPLETED) & UCP_REQUEST_FLAG_RELEASED) {
        ucp_request_put(req);
    }
    UCS_PROFILE_REQUEST_EVENT(req, "complete_recv", 0);
}

static UCS_F_ALWAYS_INLINE void
ucp_tag_recv_common(ucp_worker_h worker, void *buffer, size_t count,
                    uintptr_t datatype, ucp_tag_t tag, ucp_tag_t tag_mask,
                    ucp_request_t *req, uint32_t req_flags, ucp_tag_recv_callback_t cb,
                    ucp_recv_desc_t *rdesc, const char *debug_name)
{
    unsigned common_flags = UCP_REQUEST_FLAG_RECV | UCP_REQUEST_FLAG_EXPECTED;
//    ucp_eager_first_hdr_t *eagerf_hdr;
    ucp_request_queue_t *req_queue;
//    ucs_memory_type_t mem_type;
//    size_t hdr_len, recv_len;
//    ucs_status_t status;
//    uint64_t msg_id;

    /* Initialize receive request */
    req->status             = UCS_OK;
    req->recv.worker        = worker;
    req->recv.buffer        = buffer;
    req->recv.datatype      = datatype;

//    ucp_dt_recv_state_init(&req->recv.state, buffer, datatype, count);
    req->recv.state.dt.contig.md_map     = 0;

    req->flags              = common_flags | req_flags;
    req->recv.length        = ucp_dt_length(datatype, count, buffer,
                                            &req->recv.state);
    req->recv.mem_type      = ucp_memory_type_detect(worker->context, buffer,
                                                     req->recv.length);
    req->recv.tag.tag       = tag;
    req->recv.tag.tag_mask  = tag_mask;
    req->recv.tag.cb        = cb;
    if (ucs_log_is_enabled(UCS_LOG_LEVEL_TRACE_REQ)) {
        req->recv.tag.info.sender_tag = 0;
    }

    /* If not found on unexpected, wait until it arrives.
         * If was found but need this receive request for later completion, save it */
    req_queue = ucp_tag_exp_get_queue(&worker->tm, tag, tag_mask);
    ucp_tag_exp_push(&worker->tm, req_queue, req);
    return;
}

UCS_PROFILE_FUNC(ucs_status_t, ucp_tag_recv_nbr,
                 (worker, buffer, count, datatype, tag, tag_mask, request),
                 ucp_worker_h worker, void *buffer, size_t count,
                 uintptr_t datatype, ucp_tag_t tag, ucp_tag_t tag_mask,
                 void *request)
{
    ucp_request_t *req = (ucp_request_t *)request - 1;
    ucp_recv_desc_t *rdesc;

    UCP_CONTEXT_CHECK_FEATURE_FLAGS(worker->context, UCP_FEATURE_TAG,
                                    return UCS_ERR_INVALID_PARAM);
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(worker);

    rdesc = ucp_tag_unexp_search(&worker->tm, tag, tag_mask, 1, "recv_nbr");
    ucp_tag_recv_common(worker, buffer, count, datatype, tag, tag_mask,
                        req, UCP_REQUEST_DEBUG_FLAG_EXTERNAL, NULL, rdesc,
                        "recv_nbr");

    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(worker);
    return UCS_OK;
}

UCS_PROFILE_FUNC(ucs_status_ptr_t, ucp_tag_recv_nb,
                 (worker, buffer, count, datatype, tag, tag_mask, cb),
                 ucp_worker_h worker, void *buffer, size_t count,
                 uintptr_t datatype, ucp_tag_t tag, ucp_tag_t tag_mask,
                 ucp_tag_recv_callback_t cb)
{
    ucp_recv_desc_t *rdesc;
    ucs_status_ptr_t ret;
    ucp_request_t *req;

    UCP_CONTEXT_CHECK_FEATURE_FLAGS(worker->context, UCP_FEATURE_TAG,
                                    return UCS_STATUS_PTR(UCS_ERR_INVALID_PARAM));
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(worker);

    req = ucp_request_get(worker);
    if (ucs_likely(req != NULL)) {
        //rdesc = ucp_tag_unexp_search(&worker->tm, tag, tag_mask, 1, "recv_nb");
        rdesc = NULL;
        ucp_tag_recv_common(worker, buffer, count, datatype, tag, tag_mask, req,
                            UCP_REQUEST_FLAG_CALLBACK, cb, rdesc,"recv_nb");
        ret = req + 1;
    } else {
        ret = UCS_STATUS_PTR(UCS_ERR_NO_MEMORY);
    }

    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(worker);
    return ret;
}

UCS_PROFILE_FUNC(ucs_status_ptr_t, ucp_tag_msg_recv_nb,
                 (worker, buffer, count, datatype, message, cb),
                 ucp_worker_h worker, void *buffer, size_t count,
                 uintptr_t datatype, ucp_tag_message_h message,
                 ucp_tag_recv_callback_t cb)
{
    ucp_recv_desc_t *rdesc = message;
    ucs_status_ptr_t ret;
    ucp_request_t *req;

    UCP_CONTEXT_CHECK_FEATURE_FLAGS(worker->context, UCP_FEATURE_TAG,
                                    return UCS_STATUS_PTR(UCS_ERR_INVALID_PARAM));
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(worker);

    req = ucp_request_get(worker);
    if (ucs_likely(req != NULL)) {
        ucp_tag_recv_common(worker, buffer, count, datatype,
                            ucp_rdesc_get_tag(rdesc), UCP_TAG_MASK_FULL, req,
                            UCP_REQUEST_FLAG_CALLBACK, cb, rdesc, "msg_recv_nb");
        ret = req + 1;
    } else {
        ret = UCS_STATUS_PTR(UCS_ERR_NO_MEMORY);
    }

    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(worker);
    return ret;
}
