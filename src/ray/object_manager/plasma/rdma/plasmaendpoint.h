#pragma once

#include <thread>
#include <unordered_map>

#include "ray/util/logging.h"
#include "rdmapeer.h"
#include "rpc.h"

constexpr int NUM_LISTEN_PORTS = 4;
constexpr int LISTEN_PORTS_START = 30000;
constexpr int TWOSIDED_PERMISSIONS = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_RELAXED_ORDERING;
constexpr int ONESIDED_PERMISSIONS = IBV_ACCESS_LOCAL_WRITE |
                                     IBV_ACCESS_RELAXED_ORDERING |
                                     IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

class LocalMR {
 public:
  LocalMR() = delete;
  template <typename T>
  LocalMR(T *laddr, size_t length)
      : laddr_(reinterpret_cast<uint8_t *>(laddr)), length_(length) {}

  // LocalMR are supposed to be initialized, and then caller calls
  // set_registered() once it's been registered
  void set_registered(ibv_mr *rdma) {
    assert(laddr_ && length_ && !registered_);

    rdma_ = rdma;
    registered_ = true;
  }

  auto *laddr() const { return laddr_; }
  auto length() const { return length_; }
  auto rkey() const {
    assert(registered_);
    return rdma_->rkey;
  }
  auto lkey() const {
    assert(registered_);
    return rdma_->lkey;
  }
  const ibv_mr *rdma() { return rdma_; }

 private:
  uint8_t *laddr_ = nullptr;
  size_t length_ = 0;
  // this is owned by RDMAPeer's registered_mrs
  ibv_mr *rdma_ = nullptr;
  bool registered_ = false;
};

struct PeerMR {
 public:
  void set_rdma(const ibv_mr *rdma) { memcpy(&rdma_, rdma, sizeof(rdma_)); }
  uint8_t *raddr() { return static_cast<uint8_t *>(rdma_.addr); }
  auto length() const { return rdma_.length; }
  auto rkey() const { return rdma_.rkey; }
  auto lkey() const { return rdma_.lkey; }

 private:
  // we own the ibv_mr but this is just a copy of the peer's
  ibv_mr rdma_;
};

class EndpointManager;
// There's two ways to connect a PlasmaEndpoint: First, actively, by calling
// connect() on it.  Second, passively by calling start_listen_thread() and
// receiving a connection request.
class PlasmaEndpoint : public RDMAPeer {
 public:
  enum class EpType { INVALID, ACTIVE, PASSIVE };

  // num_qps = 1, num_cqs = 1
  PlasmaEndpoint(EndpointManager *mgr, LocalMR unreg_local_mr)
      : RDMAPeer(1, 1),
        local_mr_(unreg_local_mr.laddr(), unreg_local_mr.length()),
        sendreq_mr_(&sendreq_, sizeof(sendreq_)),
        recvreq_mr_(&recvreq_, sizeof(recvreq_)) {}

  void start_listen_thread(int port, EndpointManager *mgr);
  void stop_unconnected_listen_thread();
  void wait_connected_listen_thread();
  void active_connect(const std::string &ip, int port);
  void disconnect();
  std::string get_peer_ip() const;
  bool connected();
  EpType eptype() const { return eptype_; }

 private:
  void thread_connect_passive(unsigned int port, EndpointManager *mgr);
  void handle_addr_resolved(RDMAContext &ctx, rdma_cm_id *cm_id);
  void handle_conn_request(RDMAContext &ctx, rdma_cm_id *cm_id);
  // handles the final step of connection establishment; assumes
  // connected_active_ is already set
  void handle_conn_established(RDMAContext &ctx);
  void exchange_mrs();
  void register_twosided(LocalMR &mr);
  void register_onesided(LocalMR &mr);
  void post_send_ctrlreq_poll(const CtrlReq &req, uint32_t lkey);
  void post_recv_ctrlreq(CtrlReq &req, uint32_t lkey);

  // Once an endpoint has started a connection flow, whoever is connecting it
  // (listen thread or active_connect()) sets its type to ACTIVE or PASSIVE
  EpType eptype_ = EpType::INVALID;

  bool stop_thread_ = false;
  std::unique_ptr<std::thread> listen_thread_;

  // PeerMR information is received from peer during connection bootstrap
  PeerMR peer_mr_;
  // 1-sided RDMA buffer this endpoint will be able to access. Kept here
  // because we need to register the buffer to every connected endpoint. TODO:
  // if needed, add support for multiple regions.
  LocalMR local_mr_;

  // For basic RPCs. We can only send one req at a time, but we can receive
  // many.
  CtrlReq sendreq_;
  LocalMR sendreq_mr_;
  std::array<CtrlReq, QP_MAX_2SIDED_WRS> recvreq_;
  LocalMR recvreq_mr_;
};

class EndpointManager {
 public:
  EndpointManager(LocalMR unreg_local_mr) : unreg_local_mr_(unreg_local_mr) {}

  // creates one PlasmaEndpoint per NUM_LISTEN_PORTS and listens for incoming
  // connections
  void listen_start(int start_port);
  // The normal disconnect process is started by the active peer. Thus, this
  // function stops the threads that are not yet connected, and the ones that
  // have connected are kept alive waiting for disconnect process to be
  // triggered by the peer.
  void listen_stop_notconnected();
  // wait for the actively connected peers (not us) to trigger disconnect
  // process
  void listen_wait_connected();
  // connect to the given ip and port, and create an Endpoint that corresponds
  // to the given IP
  void connect_to(const std::string &ip, int port);
  // get the ep connected to a given IP; return nullptr if not found
  PlasmaEndpoint *get_ep_to(const std::string &ip);
  size_t num_connected_eps() const { return ips_to_connected_eps_.size(); }
  void add_connected_ep(const std::string &ip, PlasmaEndpoint *ep) {
    ips_to_connected_eps_.emplace(ip, ep);
  }

 private:
  // storage for PlasmaEndpoints; not all of these are connected eps
  std::vector<std::unique_ptr<PlasmaEndpoint>> eps_;
  std::unordered_map<std::string, PlasmaEndpoint *> ips_to_connected_eps_;

  // an unregistered local mr that all endpoints will register
  LocalMR unreg_local_mr_;
};