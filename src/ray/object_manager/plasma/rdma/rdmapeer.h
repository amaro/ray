#pragma once

#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>

#include <cassert>
#include <list>
#include <memory>
#include <queue>
#include <vector>

#include "ray/util/logging.h"

namespace plasma {
struct RDMAContext;

static constexpr uint32_t QP_MAX_2SIDED_WRS = 256;
/* reads are limited to 16 by hw */
static constexpr uint32_t QP_MAX_1SIDED_WRS = 16;
static constexpr uint32_t CQ_MAX_OUTSTANDING_CQES = 16;
/* TODO: try larger values of batched recvs */
static constexpr uint32_t MAX_BATCHED_RECVS = 16;
/* TODO: find a better place for this */
inline thread_local uint16_t current_tid;

template <typename T>
inline void inc_with_wraparound(T &ref, const T &maxvalue) {
  if (++ref >= maxvalue) ref = 0;
}

/* wraps ibv_cq_ex's with batched polling */
struct CompQueue {
  ibv_cq_ex *cqx = nullptr;
  bool poll_started = false;
  unsigned int outstanding_cqes = 0;

  /* returns either 0: which means we can read the comp, or ENOENT: which means
   * there was no completion available. any other return code ends the program.
   */
  int start_poll() {
    assert(!poll_started);
    struct ibv_poll_cq_attr cq_attr = {};

    int ret = ibv_start_poll(cqx, &cq_attr);
    switch (ret) {
    case 0:
    case ENOENT:
      poll_started = true;
      return ret;
    default:
      RAY_LOG(ERROR) << "ibv_start_poll() returned " << ret;
    }
  }

  void maybe_end_poll(unsigned int newpolls) {
    if (!poll_started) return;

    outstanding_cqes += newpolls;

    if (outstanding_cqes >= CQ_MAX_OUTSTANDING_CQES) {
      ibv_end_poll(cqx);
      outstanding_cqes = 0;
      poll_started = false;
    }
  }
};

class RDMAPeer {
 protected:
  std::vector<RDMAContext> contexts;
  ibv_context *dev_ctx;
  ibv_pd *pd;

  // cqs are distributed evenly across qps
  // e.g., if num_qps=4 and num_cqs=2, cq=0 is assigned to qp=0 and qp=1
  // and cq=1 is assigned to qp=2 and qp=3
  std::unique_ptr<CompQueue[]> send_cqs;
  std::unique_ptr<CompQueue[]> recv_cqs;
  std::vector<RDMAContext *> curr_batch_ctxs;

  bool pds_cqs_created = false;
  uint32_t unsignaled_sends = 0;
  uint16_t num_created_qps = 0;
  uint16_t num_qps;
  uint16_t num_cqs;
  uint16_t qps_per_thread;

  std::list<ibv_mr *> registered_mrs;

  void create_pds_cqs(ibv_context *verbs);
  void destroy_pds_cqs();
  void create_qps(RDMAContext &ctx);
  void connect_or_accept(RDMAContext &ctx, bool connect);
  void dereg_mrs();
  void handle_conn_established(RDMAContext &ctx);

 public:
  static constexpr int TIMEOUT_MS = 5;
  static constexpr int QP_ATTRS_MAX_SGE_ELEMS = 1;
  static constexpr int QP_ATTRS_MAX_INLINE_DATA = 256;
  static constexpr uint32_t MAX_UNSIGNALED_SENDS = 16;
  // static constexpr int MAX_QP_INFLIGHT_READS = 16;  // hw limited

  RDMAPeer(uint16_t num_qps, uint16_t num_cqs)
      : send_cqs(std::unique_ptr<CompQueue[]>(new CompQueue[num_cqs])),
        recv_cqs(std::unique_ptr<CompQueue[]>(new CompQueue[num_cqs])),
        curr_batch_ctxs(num_cqs),
        num_qps(num_qps),
        num_cqs(num_cqs),
        qps_per_thread(num_qps / num_cqs) {
    static_assert(MAX_UNSIGNALED_SENDS < QP_MAX_2SIDED_WRS);
    assert(num_qps % num_cqs == 0);
    assert(qps_per_thread > 0);
  }

  virtual ~RDMAPeer() {
    for (ibv_mr *mr : registered_mrs) ibv_dereg_mr(mr);
    registered_mrs.clear();
  }

  RDMAPeer(const RDMAPeer &) = delete;
  RDMAPeer &operator=(const RDMAPeer &) = delete;
  RDMAPeer(RDMAPeer &&source) = delete;

  ibv_mr *register_mr(void *addr, size_t len, int permissions);
  void post_recv(const RDMAContext &ctx,
                 const void *laddr,
                 uint32_t len,
                 uint32_t lkey) const;
  void post_batched_recv(RDMAContext &ctx,
                         ibv_mr *mr,
                         uint32_t startidx,
                         uint32_t per_buf_bytes,
                         uint32_t num_bufs) const;
  void post_send(const RDMAContext &ctx,
                 const void *laddr,
                 uint32_t len,
                 uint32_t lkey) const;
  /* posts an unsignaled 2-sided send,
     returns whether send_cqx should be polled */
  bool post_2s_send_unsig(const RDMAContext &ctx,
                          const void *laddr,
                          uint32_t len,
                          uint32_t lkey);
  void post_batched_send(RDMAContext &ctx,
                         const void *laddr,
                         uint32_t len,
                         uint32_t lkey);

  template <typename T>
  unsigned int poll_atleast(unsigned int times, ibv_cq_ex *cq, T &&comp_func);
  void poll_exactly(unsigned int times, ibv_cq_ex *cq);

  template <typename T>
  unsigned int poll_atmost(unsigned int max, ibv_cq_ex *cq, T &&comp_func);
  template <typename T>
  unsigned int poll_batched_atmost(unsigned int max,
                                   CompQueue &comp_queue,
                                   T &&comp_func);
  // these functions receive tids because some users (e.g., RDMAServer) are
  // created per thread, so they only have 1 cq. in those cases, indexing the cq
  // by thread id would be wrong.
  ibv_cq_ex *get_send_cq(uint16_t tid);
  ibv_cq_ex *get_recv_cq(uint16_t tid);
  CompQueue &get_send_compqueue(uint16_t tid);
  CompQueue &get_recv_compqueue(uint16_t tid);

  std::vector<RDMAContext> &get_contexts();
  RDMAContext &get_next_context();
  RDMAContext &get_context(uint16_t ctx_id);
  RDMAContext &get_ctrl_ctx();
  uint16_t get_num_qps();

  void start_batched_ops(RDMAContext *ctx);
  void end_batched_ops();
  RDMAContext *get_batch_ctx();
  bool memqueues_empty();
};

/* TODO: move this to own file */
struct RDMAContext {
  struct SendOp {
    uint64_t wr_id;
    const void *laddr;
    unsigned int len;
    unsigned int lkey;
    bool signaled;
  };

  struct OneSidedOp {
    enum class OpType { INVALID, READ, WRITE, CMP_SWP, FETCH_ADD };

    uintptr_t raddr = 0;
    uintptr_t laddr = 0;
    uint32_t len = 0;
    uint32_t rkey = 0;
    uint32_t lkey = 0;
    uint64_t cmp = 0;
    uint64_t swp = 0;
    OpType optype = OpType::INVALID;

    void post(ibv_qp_ex *qpx, unsigned int flags, uint64_t wr_id) {
      qpx->wr_flags = flags;
      qpx->wr_id = wr_id;

      switch (optype) {
      case OpType::READ:
        ibv_wr_rdma_read(qpx, rkey, raddr);
        break;
      case OpType::WRITE:
        ibv_wr_rdma_write(qpx, rkey, raddr);
        break;
      case OpType::CMP_SWP:
        ibv_wr_atomic_cmp_swp(qpx, rkey, raddr, cmp, swp);
        break;
      case OpType::FETCH_ADD:
      case OpType::INVALID:
        RAY_LOG(ERROR) << "bad optype";
      }

      ibv_wr_set_sge(qpx, lkey, laddr, len);
    }
  };

  struct RecvOp {
    ibv_recv_wr wr;
    ibv_sge sge;
  };

  enum BatchType { SEND, ONESIDED };

  /* TODO: move this inside SendOp */
  void _post_send(SendOp &sendop) {
    if (sendop.signaled)
      qpx->wr_flags = IBV_SEND_SIGNALED;
    else
      qpx->wr_flags = 0;

    qpx->wr_id = sendop.wr_id;
    ibv_wr_send(qpx);
    ibv_wr_set_sge(qpx, sendop.lkey, (uintptr_t)sendop.laddr, sendop.len);
    outstanding_sends++;
  }

 public:
  uint32_t ctx_id; /* TODO: rename to id */
  bool connected = false;
  uint32_t outstanding_sends = 0;
  /* Number of sends or onesided ops posted that have not been completed.
   * TODO: Accessed directly from scheduler.
   * Need to be careful not to double count, so an op must only be counted once
   * in newbatch_ops or oustanding_ops*/
  uint32_t outstanding_onesided = 0;
  /* Number of ops in currently new batch */
  uint32_t newbatch_ops = 0;

  rdma_cm_id *cm_id = nullptr;
  ibv_qp *qp = nullptr;
  ibv_qp_ex *qpx = nullptr;
  rdma_event_channel *event_channel = nullptr;

  SendOp buffered_send;
  OneSidedOp buffered_onesided;
  BatchType curr_batch_type;

  /* TODO: std::array<RecvOp> */
  std::vector<RecvOp> recv_batch;

  RDMAContext(unsigned int ctx_id)
      : ctx_id{ctx_id}, buffered_send{0, nullptr, 0, 0, false} {
    recv_batch.reserve(MAX_BATCHED_RECVS);
    for (auto i = 0u; i < MAX_BATCHED_RECVS; ++i) {
      recv_batch.emplace_back(RecvOp());
    }
  }

  void disconnect() {
    assert(connected);
    connected = false;

    ibv_destroy_qp(qp);
    rdma_destroy_id(cm_id);
    rdma_destroy_event_channel(event_channel);
  }

  void post_batched_send(const void *laddr, unsigned int len, unsigned int lkey) {
    RAY_CHECK(outstanding_sends < QP_MAX_2SIDED_WRS);

    /* if this is not the first WR within the batch, post the previously
     * buffered send */
    if (newbatch_ops > 0)
      _post_send(buffered_send);
    else
      curr_batch_type = BatchType::SEND;

    buffered_send.wr_id = 0;
    buffered_send.laddr = laddr;
    buffered_send.len = len;
    buffered_send.lkey = lkey;
    buffered_send.signaled = false;

    newbatch_ops++;
  }

  /* post the last send of a batch */
  void end_batched_sends() {
    buffered_send.signaled = true;
    buffered_send.wr_id = newbatch_ops;
    _post_send(buffered_send);
  }

  /* Posts a onesided op. */
  void post_batched_onesided(OneSidedOp op) {
    if (newbatch_ops > 0)
      buffered_onesided.post(qpx, 0, 0);
    else
      curr_batch_type = BatchType::ONESIDED;

    newbatch_ops++;
    buffered_onesided = op;
    assert(newbatch_ops + outstanding_onesided <= QP_MAX_1SIDED_WRS);
  }

  void end_batched_onesided() {
    uint64_t wr_id = ctx_id;
    /* left 32 bits used for ctx_id, right 32 bits for the number of rmcs to
     * awake when onesided batch completes */
    wr_id <<= 32;
    wr_id |= newbatch_ops;
    buffered_onesided.post(qpx, IBV_SEND_SIGNALED, wr_id);
    outstanding_onesided += newbatch_ops;
  }

  uint16_t free_onesided_slots() const {
    assert(outstanding_onesided + newbatch_ops <= QP_MAX_1SIDED_WRS);
    return QP_MAX_1SIDED_WRS - newbatch_ops - outstanding_onesided;
  }

  void complete_onesided(uint16_t comp) {
    assert(outstanding_onesided >= comp);
    outstanding_onesided -= comp;
  }

  void complete_send(uint16_t comp) {
    assert(outstanding_sends >= comp);
    outstanding_sends -= comp;
  }

  /* arguments:
         laddr is the starting local address
         req_len is the individual recv request length in bytes
         total_reqs is the total number of recv reqs that will be posted
         lkey is the local key for the memory region the recv will access
  */
  void post_batched_recv(uintptr_t laddr,
                         uint32_t req_len,
                         uint32_t total_reqs,
                         uint32_t lkey) {
    ibv_recv_wr *bad_wr = nullptr;
    int err = 0;

    RAY_CHECK(total_reqs <= MAX_BATCHED_RECVS);

    for (auto i = 0u; i < total_reqs; ++i) {
      recv_batch[i].sge.addr = laddr + i * req_len;
      recv_batch[i].sge.length = req_len;
      recv_batch[i].sge.lkey = lkey;

      recv_batch[i].wr.sg_list = &recv_batch[i].sge;
      recv_batch[i].wr.num_sge = 1;

      if (i == total_reqs - 1)
        recv_batch[i].wr.next = nullptr;
      else
        recv_batch[i].wr.next = &recv_batch[i + 1].wr;
    }

    err = ibv_post_recv(qp, &recv_batch[0].wr, &bad_wr);
    RAY_CHECK(err == 0);
  }

  void start_batch() {
    assert(newbatch_ops == 0);

    ibv_wr_start(qpx);
  }

  void end_batch() {
    /* if we are in the middle of a batched op, end it */
    if (newbatch_ops > 0) {
      switch (curr_batch_type) {
      case BatchType::SEND:
        end_batched_sends();
        break;
      case BatchType::ONESIDED:
        end_batched_onesided();
        break;
      default:
        RAY_LOG(ERROR) << "unrecognized batch type";
      }
    }

    RAY_CHECK(ibv_wr_complete(qpx) == 0);
    newbatch_ops = 0;
  }

  void post_single_onesided(OneSidedOp op) {
    ibv_wr_start(qpx);
    op.post(qpx, IBV_SEND_SIGNALED, 0);
    RAY_CHECK(ibv_wr_complete(qpx) == 0);
  }
};

inline ibv_mr *RDMAPeer::register_mr(void *addr, size_t len, int permissions) {
  assert(addr && len && permissions);

  ibv_mr *mr = ibv_reg_mr(pd, addr, len, permissions);
  if (!mr) RAY_LOG(ERROR) << "could not register mr";

  registered_mrs.push_back(mr);
  return mr;
}

inline void RDMAPeer::post_recv(const RDMAContext &ctx,
                                const void *laddr,
                                uint32_t len,
                                uint32_t lkey) const {
  int err = 0;
  ibv_sge sge = {.addr = reinterpret_cast<uintptr_t>(laddr), .length = len, .lkey = lkey};

  ibv_recv_wr wr = {};
  ibv_recv_wr *bad_wr = nullptr;

  wr.next = nullptr;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  err = ibv_post_recv(ctx.qp, &wr, &bad_wr);
  RAY_CHECK(err == 0);
}

inline void RDMAPeer::post_batched_recv(RDMAContext &ctx,
                                        ibv_mr *mr,
                                        uint32_t startidx,
                                        uint32_t per_buf_bytes,
                                        uint32_t num_bufs) const {
  uint32_t max_batch_size = MAX_BATCHED_RECVS;
  uint32_t num_batches = num_bufs / max_batch_size;
  uint32_t batch_size = 0;
  uintptr_t base_addr =
      reinterpret_cast<uintptr_t>(mr->addr) + (startidx * per_buf_bytes);

  if (num_bufs % max_batch_size != 0) num_batches++;

  for (auto batch = 0u; batch < num_batches; ++batch) {
    if (num_bufs > (batch + 1) * max_batch_size)
      batch_size = max_batch_size;
    else
      batch_size = num_bufs - (batch * max_batch_size);

    ctx.post_batched_recv(base_addr + (batch * max_batch_size * per_buf_bytes),
                          per_buf_bytes,
                          batch_size,
                          mr->lkey);
  }
}

inline void RDMAPeer::post_send(const RDMAContext &ctx,
                                const void *laddr,
                                uint32_t len,
                                uint32_t lkey) const {
  ibv_wr_start(ctx.qpx);
  ctx.qpx->wr_flags = IBV_SEND_SIGNALED;
  ibv_wr_send(ctx.qpx);
  ibv_wr_set_sge(ctx.qpx, lkey, reinterpret_cast<uintptr_t>(laddr), len);
  RAY_CHECK(ibv_wr_complete(ctx.qpx) == 0);
}

/* returns whether the current posted send was signaled.
   the caller must make sure that we only attempt polling the
   signaled send after it has been posted */
inline bool RDMAPeer::post_2s_send_unsig(const RDMAContext &ctx,
                                         const void *laddr,
                                         uint32_t len,
                                         uint32_t lkey) {
  bool signaled = false;
  int ret;

  if (this->unsignaled_sends + 1 == MAX_UNSIGNALED_SENDS) signaled = true;

  ibv_wr_start(ctx.qpx);

  if (signaled)
    ctx.qpx->wr_flags = IBV_SEND_SIGNALED;
  else
    ctx.qpx->wr_flags = 0;

  ibv_wr_send(ctx.qpx);
  ibv_wr_set_sge(ctx.qpx, lkey, (uintptr_t)laddr, len);

  ret = ibv_wr_complete(ctx.qpx);
  RAY_CHECK(ret == 0);

  inc_with_wraparound(this->unsignaled_sends, MAX_UNSIGNALED_SENDS);
  return signaled;
}

inline ibv_cq_ex *RDMAPeer::get_send_cq(uint16_t tid) {
  return get_send_compqueue(tid).cqx;
}

inline ibv_cq_ex *RDMAPeer::get_recv_cq(uint16_t tid) {
  return get_recv_compqueue(tid).cqx;
}

inline CompQueue &RDMAPeer::get_send_compqueue(uint16_t tid) {
  assert(tid <= num_cqs);
  return send_cqs[tid];
}

inline CompQueue &RDMAPeer::get_recv_compqueue(uint16_t tid) {
  assert(tid <= num_cqs);
  return recv_cqs[tid];
}

inline std::vector<RDMAContext> &RDMAPeer::get_contexts() { return contexts; }

// this could potentially allow any thread to get any context, so be careful
// when using ctx_ids to select the context
inline RDMAContext &RDMAPeer::get_context(uint16_t ctx_id) { return contexts[ctx_id]; }

// if qps_per_thread == 1, always returns the tid-th context
// if qps_per_thread > 1, alternates between
//   [qps_per_thread * tid, qps_per_thread * tid + 1,...,qps_per_thread * tid +
//   (qps_per_thread - 1)]
inline RDMAContext &RDMAPeer::get_next_context() {
  thread_local uint16_t per_thread_idx = 0;

  if (qps_per_thread == 1) return contexts[current_tid];

  RDMAContext &ctx = contexts[qps_per_thread * current_tid + per_thread_idx];
  inc_with_wraparound(per_thread_idx, qps_per_thread);
  return ctx;
}

/* used for send/recvs. we could have an independent qp only for these ops
   but probably not worth the code */
inline RDMAContext &RDMAPeer::get_ctrl_ctx() { return contexts[0]; }

inline uint16_t RDMAPeer::get_num_qps() { return num_qps; }

template <typename T>
inline unsigned int RDMAPeer::poll_atmost(unsigned int max,
                                          ibv_cq_ex *cq,
                                          T &&comp_func) {
  int ret;
  unsigned int polled = 0;
  struct ibv_poll_cq_attr cq_attr = {};

  assert(max > 0);

  while ((ret = ibv_start_poll(cq, &cq_attr)) != 0) {
    if (ret == ENOENT)
      return 0;
    else
      RAY_LOG(ERROR) << "ibv_start_poll() returned " << ret;
  }

  do {
    if (polled > 0) {
      while ((ret = ibv_next_poll(cq)) != 0) {
        if (ret == ENOENT)
          goto end_poll;
        else
          RAY_LOG(ERROR) << "ibv_next_poll() returned " << ret;
      }
    }

    RAY_CHECK(cq->status == IBV_WC_SUCCESS);

    /* the post-completion function takes wr_id,
     * for reads, this is the ctx_id,
     * for sends, this is the batch size */
    comp_func(cq->wr_id);

    polled++;
  } while (polled < max);

end_poll:
  ibv_end_poll(cq);
  assert(polled <= max);
  return polled;
}

template <typename T>
inline unsigned int RDMAPeer::poll_batched_atmost(unsigned int max,
                                                  CompQueue &comp_queue,
                                                  T &&comp_func) {
  int ret;
  unsigned int polled = 0;

  assert(max > 0);

  if (!comp_queue.poll_started) {
    ret = comp_queue.start_poll();
    if (ret == ENOENT)  // no comp available
      goto end_poll;
    else  // comp available, read it
      goto read;
  }

  do {
    ret = ibv_next_poll(comp_queue.cqx);
    if (ret == ENOENT)
      goto end_poll;
    else if (ret != 0)
      RAY_LOG(ERROR) << "ibv_next_poll() returned " << ret;

  read:
    RAY_CHECK(comp_queue.cqx->status == IBV_WC_SUCCESS);

    /* the post-completion function takes wr_id,
     * for reads, this is the ctx_id,
     * for sends, this is the batch size */
    comp_func(comp_queue.cqx->wr_id);

    polled++;
  } while (polled < max);

end_poll:
  comp_queue.maybe_end_poll(polled);
  assert(polled <= max);
  return polled;
}

inline void RDMAPeer::poll_exactly(unsigned int target, ibv_cq_ex *cq) {
  int ret;
  unsigned int polled = 0;
  struct ibv_poll_cq_attr cq_attr = {};

  while ((ret = ibv_start_poll(cq, &cq_attr)) != 0) {
    if (ret == ENOENT)
      continue;
    else
      RAY_LOG(ERROR) << "ibv_start_poll() returned " << ret;
  }

  do {
    if (polled > 0) {
      while ((ret = ibv_next_poll(cq)) != 0) {
        if (ret == ENOENT)
          continue;
        else
          RAY_LOG(ERROR) << "ibv_next_poll() returned " << ret;
      }
    }

    RAY_CHECK(cq->status == IBV_WC_SUCCESS);
    polled++;
  } while (polled < target);

  ibv_end_poll(cq);
}

template <typename T>
inline unsigned int RDMAPeer::poll_atleast(unsigned int target,
                                           ibv_cq_ex *cq,
                                           T &&comp_func) {
  int ret;
  unsigned int polled = 0;
  struct ibv_poll_cq_attr cq_attr = {};

  while ((ret = ibv_start_poll(cq, &cq_attr)) != 0) {
    if (ret == ENOENT)
      continue;
    else
      RAY_LOG(ERROR) << "error in ibv_start_poll()";
  }

read:
  if (cq->status != IBV_WC_SUCCESS) RAY_LOG(ERROR) << "cq status is not success";

  /* the post-completion function takes wr_id,
   * for sends, this is the batch size */
  comp_func(cq->wr_id);

  polled++;

next_poll:
  ret = ibv_next_poll(cq);
  if (ret == 0) {
    goto read;
  } else if (ret == ENOENT) {
    if (polled < target)
      goto next_poll; /* we haven't reached the target, retry. */
    else
      goto out; /* reached target, we can leave */
  } else {
    RAY_LOG(ERROR) << "error in ibv_next_poll()";
  }

out:
  ibv_end_poll(cq);
  return polled;
}

inline void RDMAPeer::start_batched_ops(RDMAContext *ctx) {
  assert(curr_batch_ctxs[current_tid] == nullptr);

  curr_batch_ctxs[current_tid] = ctx;
  ctx->start_batch();
}

/* end batched ops (reads/writes/sends) */
inline void RDMAPeer::end_batched_ops() {
  assert(curr_batch_ctxs[current_tid] != nullptr);

  curr_batch_ctxs[current_tid]->end_batch();
  curr_batch_ctxs[current_tid] = nullptr;
}

inline RDMAContext *RDMAPeer::get_batch_ctx() { return curr_batch_ctxs[current_tid]; }

}  // namespace plasma