#pragma once

#include <infiniband/verbs.h>

struct MrReq {
  ibv_mr mr;
};

struct ConsumeReq {
  uint64_t loffset;
};

struct WriteReq {
  uint64_t roffset;
  uint64_t loffset;
  uint32_t size;
};

enum CtrlCmdType { RDMA_MR = 1, CONSUME, REQWRITE };

/* Control path request */
struct CtrlReq {
  CtrlCmdType type;

  union {
    MrReq mr;
    ConsumeReq consumreq;
    WriteReq writereq;
  } data;
};
