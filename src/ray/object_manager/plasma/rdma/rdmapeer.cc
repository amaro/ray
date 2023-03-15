#include "ray/object_manager/plasma/rdma/rdmapeer.h"

namespace plasma {
void RDMAPeer::create_pds_cqs(ibv_context *verbs) {
  ibv_cq_init_attr_ex cq_attrs_ex = {};
  cq_attrs_ex.cqe = QP_MAX_2SIDED_WRS;
  cq_attrs_ex.comp_vector = 0;
  cq_attrs_ex.comp_mask = IBV_CQ_INIT_ATTR_MASK_FLAGS;
  cq_attrs_ex.flags = IBV_CREATE_CQ_ATTR_SINGLE_THREADED;

  dev_ctx = verbs; /* TODO: is this needed? */
  RAY_CHECK((pd = ibv_alloc_pd(dev_ctx)) != nullptr);

  for (auto i = 0u; i < num_cqs; ++i) {
    RAY_CHECK((send_cqs[i].cqx = mlx5dv_create_cq(dev_ctx, &cq_attrs_ex, nullptr)) !=
              nullptr);
    RAY_CHECK((recv_cqs[i].cqx = mlx5dv_create_cq(dev_ctx, &cq_attrs_ex, nullptr)) !=
              nullptr);
  }

  pds_cqs_created = true;
  RAY_LOG(DEBUG) << "created pds and " << num_cqs << " cqs";
}

void RDMAPeer::destroy_pds_cqs() {
  RAY_CHECK(pds_cqs_created);
  pds_cqs_created = false;

  ibv_dealloc_pd(pd);
  for (auto i = 0u; i < num_cqs; ++i) {
    ibv_destroy_cq(ibv_cq_ex_to_cq(get_send_cq(i)));
    ibv_destroy_cq(ibv_cq_ex_to_cq(get_recv_cq(i)));
  }
}

void RDMAPeer::create_qps(RDMAContext &ctx) {
  ibv_qp_init_attr_ex qp_attrs = {};
  const uint16_t qps_per_cq = num_qps / num_cqs;
  const uint16_t cq_idx = num_created_qps / qps_per_cq;

  // cannot use current_tid here because OneSidedClient is shared among threads,
  // so current_tid will be 0 for all qps. so we keep track of how many qps
  // we have created so far, and assign cq_idxs evenly to them
  ibv_cq_ex *recv_cq = get_recv_cq(cq_idx);
  ibv_cq_ex *send_cq = get_send_cq(cq_idx);

  qp_attrs.send_cq = ibv_cq_ex_to_cq(send_cq);
  qp_attrs.recv_cq = ibv_cq_ex_to_cq(recv_cq);
  qp_attrs.qp_type = IBV_QPT_RC;
  qp_attrs.sq_sig_all = 0;
  qp_attrs.pd = this->pd;
  /* identified valid fields? */
  qp_attrs.comp_mask |= IBV_QP_INIT_ATTR_SEND_OPS_FLAGS | IBV_QP_INIT_ATTR_PD;

  qp_attrs.cap.max_send_wr = QP_MAX_2SIDED_WRS;
  qp_attrs.cap.max_recv_wr = QP_MAX_2SIDED_WRS;
  qp_attrs.cap.max_send_sge = QP_ATTRS_MAX_SGE_ELEMS;
  qp_attrs.cap.max_recv_sge = QP_ATTRS_MAX_SGE_ELEMS;
  qp_attrs.cap.max_inline_data = QP_ATTRS_MAX_INLINE_DATA;

  qp_attrs.send_ops_flags |= IBV_QP_EX_WITH_RDMA_READ | IBV_QP_EX_WITH_RDMA_WRITE |
                             IBV_QP_EX_WITH_ATOMIC_CMP_AND_SWP | IBV_WR_SEND;

  RAY_CHECK((ctx.qp = mlx5dv_create_qp(ctx.cm_id->verbs, &qp_attrs, NULL)) != nullptr);
  ctx.cm_id->qp = ctx.qp;
  ctx.qpx = ibv_qp_to_qp_ex(ctx.qp);

  RAY_LOG(DEBUG) << "created qp=" << ctx.qp << " bound to send_cq=" << send_cq
                 << " and recv_cq=" << recv_cq;
  num_created_qps++;
}

void RDMAPeer::connect_or_accept(RDMAContext &ctx, bool connect) {
  ibv_device_attr attrs = {};
  rdma_conn_param cm_params = {};

  ibv_query_device(ctx.cm_id->verbs, &attrs);

  cm_params.responder_resources = attrs.max_qp_init_rd_atom;
  cm_params.initiator_depth = attrs.max_qp_rd_atom;
  cm_params.retry_count = 1;
  cm_params.rnr_retry_count = 1;

  if (connect)
    RAY_CHECK(rdma_connect(ctx.cm_id, &cm_params) == 0);
  else
    RAY_CHECK(rdma_accept(ctx.cm_id, &cm_params) == 0);
}

ibv_mr *RDMAPeer::register_mr(void *addr, size_t len, int permissions) {
  RAY_CHECK(pd && addr && len && permissions);

  ibv_mr *mr = ibv_reg_mr(pd, addr, len, permissions);
  RAY_CHECK(mr) << "error registering mr";

  registered_mrs.push_back(mr);
  return mr;
}

void RDMAPeer::dereg_mrs() {
  RAY_LOG(DEBUG) << "dereg_mrs()";
  for (ibv_mr *curr_mr : registered_mrs) ibv_dereg_mr(curr_mr);

  registered_mrs.clear();
}
}  // namespace plasma