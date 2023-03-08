#include "plasmaendpoint.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>

void PlasmaEndpoint::start_listen_thread(int port, EndpointManager *mgr) {
  stop_thread_ = false;
  listen_thread_.reset(
      new std::thread(&PlasmaEndpoint::thread_connect_passive, this, port, mgr));
}

void PlasmaEndpoint::stop_unconnected_listen_thread() {
  assert(!connected() && eptype_ == EpType::PASSIVE);
  stop_thread_ = true;
  listen_thread_->join();
}

void PlasmaEndpoint::wait_connected_listen_thread() {
  assert(connected() && eptype_ == EpType::PASSIVE);
  // we join here without force-stopping the thread so that we wait for
  // disconnect events
  listen_thread_->join();
  stop_thread_ = true;
  assert(!connected() && eptype_ == EpType::INVALID);
}

void PlasmaEndpoint::active_connect(const std::string &ip, int port) {
  addrinfo *addr = nullptr;
  rdma_cm_event *event = nullptr;
  char port_str[256] = "";

  RDMAContext ctx(port);
  snprintf(port_str, sizeof port_str, "%u", port);

  RAY_CHECK(getaddrinfo(ip.c_str(), port_str, nullptr, &addr) == 0);
  RAY_CHECK((ctx.event_channel = rdma_create_event_channel()) != nullptr);
  RAY_CHECK(rdma_create_id(ctx.event_channel, &ctx.cm_id, nullptr, RDMA_PS_TCP) == 0);
  RAY_CHECK(rdma_resolve_addr(ctx.cm_id, nullptr, addr->ai_addr, TIMEOUT_MS) == 0);

  freeaddrinfo(addr);

  // set this ep as active
  eptype_ = EpType::ACTIVE;

  while (rdma_get_cm_event(ctx.event_channel, &event) == 0) {
    rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);
    event = &event_copy;

    if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED) {
      handle_addr_resolved(ctx, event->id);
    } else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
      connect_or_accept(ctx, true);  // connect
    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
      handle_conn_established(ctx);
      break;
    } else {
      RAY_LOG(ERROR) << "unknown or unexpected event " << rdma_event_str(event->event);
      RAY_CHECK(0);
    }
  }
}

void PlasmaEndpoint::disconnect() {
  assert(contexts[0].connected);

  dereg_mrs();
  contexts[0].disconnect();
  destroy_pds_cqs();
  eptype_ = EpType::INVALID;
}

std::string PlasmaEndpoint::get_peer_ip() const {
  assert(contexts[0].connected);
  char buf[INET_ADDRSTRLEN] = {};

  sockaddr *peer_addr = rdma_get_peer_addr(contexts[0].cm_id);
  assert(peer_addr->sa_family == AF_INET);
  sockaddr_in *peer_addr_in = reinterpret_cast<sockaddr_in *>(peer_addr);
  inet_ntop(AF_INET, &peer_addr_in->sin_addr, buf, INET_ADDRSTRLEN);
  return std::string(buf);
}

bool PlasmaEndpoint::connected() { return contexts.size() > 0 && contexts[0].connected; }

void PlasmaEndpoint::handle_addr_resolved(RDMAContext &ctx, rdma_cm_id *cm_id) {
  assert(!ctx.connected);

  if (!pds_cqs_created) create_pds_cqs(cm_id->verbs);

  ctx.cm_id = cm_id;
  create_qps(ctx);

  RAY_CHECK(rdma_resolve_route(cm_id, TIMEOUT_MS) == 0);
}

void PlasmaEndpoint::thread_connect_passive(unsigned int port, EndpointManager *mgr) {
  sockaddr_in addr = {};
  rdma_cm_event *event = nullptr;
  rdma_cm_id *listener = nullptr;
  int ret;

  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  printf("launched thread_connect_passive() on port %d\n", port);
  RDMAContext ctx(port);
  RAY_CHECK((ctx.event_channel = rdma_create_event_channel()) != nullptr);
  int channel_fd = ctx.event_channel->fd;

  // make the channel's fd non blocking
  int flags = fcntl(channel_fd, F_GETFL);
  RAY_CHECK(fcntl(channel_fd, F_SETFL, flags | O_NONBLOCK) != -1);

  RAY_CHECK(rdma_create_id(ctx.event_channel, &listener, nullptr, RDMA_PS_TCP) == 0);
  RAY_CHECK(rdma_bind_addr(listener, (sockaddr *)&addr) == 0);
  RAY_CHECK(rdma_listen(listener, 1) == 0);

  printf("listening on port: %d\n", port);

  struct pollfd event_poll {
    channel_fd, POLLIN, 0
  };

  // mark this endpoint as PASSIVE
  eptype_ = EpType::PASSIVE;

  // TODO: use a single thread with epoll to handle all connections
  while ((ret = poll(&event_poll, 1, TIMEOUT_MS)) >= 0) {
    if (ret == 0) {
      // If stop_threads_ == true but ret > 0, then we need handle an event
      // before stopping this thread. So do that, we'll eventually get back
      // here.
      if (stop_thread_)
        break;
      else
        continue;
    }

    RAY_CHECK(rdma_get_cm_event(ctx.event_channel, &event) == 0);

    switch (event->event) {
    case RDMA_CM_EVENT_CONNECT_REQUEST: {
      handle_conn_request(ctx, event->id);
      rdma_ack_cm_event(event);
    } break;
    case RDMA_CM_EVENT_ESTABLISHED: {
      handle_conn_established(ctx);
      mgr->add_connected_ep(get_peer_ip(), this);
      rdma_ack_cm_event(event);
    } break;
    case RDMA_CM_EVENT_DISCONNECTED: {
      rdma_ack_cm_event(event);
      disconnect();
    }
      return;
    default:
      RAY_LOG(ERROR) << "unexpected event at thread_connect_passive()";
      RAY_CHECK(0);
    }
  }

  RAY_CHECK(ret >= 0);
}

void PlasmaEndpoint::handle_conn_request(RDMAContext &ctx, rdma_cm_id *cm_id) {
  assert(!ctx.connected);
  puts("connect request");

  if (!pds_cqs_created) create_pds_cqs(cm_id->verbs);

  ctx.cm_id = cm_id;
  create_qps(ctx);

  connect_or_accept(ctx, false);  // accept
}

void PlasmaEndpoint::handle_conn_established(RDMAContext &ctx) {
  assert(!ctx.connected);
  RAY_CHECK(contexts.size() == 0);

  ctx.connected = true;
  contexts.push_back(std::move(ctx));

  exchange_mrs();
}

void PlasmaEndpoint::exchange_mrs() {
  assert(contexts[0].connected);

  // register the buffers
  register_onesided(local_mr_);
  register_twosided(recvreq_mr_);
  register_twosided(sendreq_mr_);

  // now exchange local_mr_. if we connected actively, our protocol is: post
  // recv, send(local_mr_), poll send, poll recv.  if we connected passively,
  // our protocol is: post recv, poll recv, send(local_mr_), poll send.
  if (eptype_ == EpType::ACTIVE) {
    // post a recv for peer_mr_
    post_recv_ctrlreq(recvreq_[0], recvreq_mr_.lkey());

    // send our local_mr_
    sendreq_.type = CtrlCmdType::RDMA_MR;
    memcpy(&sendreq_.data.mr.mr, local_mr_.rdma(), sizeof(sendreq_.data.mr.mr));
    post_send_ctrlreq_poll(sendreq_, sendreq_mr_.lkey());

    // poll the recv
    poll_exactly(1, get_recv_cq(0));

    // copy the recvd rdma info to peer_mr_
    RAY_CHECK(recvreq_[0].type == CtrlCmdType::RDMA_MR);
    const MrReq *mr_req = &(recvreq_[0].data.mr);
    peer_mr_.set_rdma(&mr_req->mr);
  } else if (eptype_ == EpType::PASSIVE) {
    // post a recv for peer_mr_ and poll
    post_recv_ctrlreq(recvreq_[0], recvreq_mr_.lkey());
    poll_exactly(1, get_recv_cq(0));

    // copy the recvd rdma info to peer_mr_
    RAY_CHECK(recvreq_[0].type == CtrlCmdType::RDMA_MR);
    const MrReq *mr_req = &(recvreq_[0].data.mr);
    peer_mr_.set_rdma(&mr_req->mr);

    // send our local_mr_
    sendreq_.type = CtrlCmdType::RDMA_MR;
    memcpy(&sendreq_.data.mr.mr, local_mr_.rdma(), sizeof(sendreq_.data.mr.mr));
    post_send_ctrlreq_poll(sendreq_, sendreq_mr_.lkey());
  } else {
    RAY_LOG(ERROR) << "invalid eptype";
    RAY_CHECK(0);
  }

  printf("ep=%p received peer's region raddr=%p len=%lu\n",
         static_cast<void *>(this),
         peer_mr_.raddr(),
         peer_mr_.length());
}

void PlasmaEndpoint::register_twosided(LocalMR &mr) {
  ibv_mr *rdma = register_mr(mr.laddr(), mr.length(), TWOSIDED_PERMISSIONS);
  RAY_CHECK(rdma);
  mr.set_registered(rdma);
}

void PlasmaEndpoint::register_onesided(LocalMR &mr) {
  ibv_mr *rdma = register_mr(mr.laddr(), mr.length(), ONESIDED_PERMISSIONS);
  RAY_CHECK(rdma);
  mr.set_registered(rdma);
}

void PlasmaEndpoint::post_send_ctrlreq_poll(const CtrlReq &req, uint32_t lkey) {
  auto &ctx = get_ctrl_ctx();
  ctx.start_batch();
  ctx.post_batched_send(&req, sizeof(CtrlReq), lkey);
  ctx.end_batch();

  poll_exactly(1, get_send_cq(0));
  ctx.complete_send(1);
}

void PlasmaEndpoint::post_recv_ctrlreq(CtrlReq &req, uint32_t lkey) {
  post_recv(get_ctrl_ctx(), &req, sizeof(CtrlReq), lkey);
}

void EndpointManager::listen_start(int start_port) {
  for (auto i = 0; i < NUM_LISTEN_PORTS; i++) {
    std::unique_ptr<PlasmaEndpoint> ep =
        std::make_unique<PlasmaEndpoint>(this, unreg_local_mr_);
    ep->start_listen_thread(start_port + i, this);
    eps_.push_back(std::move(ep));
  }
}

void EndpointManager::listen_stop_notconnected() {
  for (auto &ep : eps_) {
    if (ep->eptype() == PlasmaEndpoint::EpType::PASSIVE && !ep->connected())
      ep->stop_unconnected_listen_thread();
  }
}

void EndpointManager::listen_wait_connected() {
  for (auto &ep : eps_) {
    if (ep->eptype() == PlasmaEndpoint::EpType::PASSIVE && ep->connected()) {
      std::string peer_ip = ep->get_peer_ip();
      ep->wait_connected_listen_thread();
      ips_to_connected_eps_.erase(peer_ip);
    }
  }
}

void EndpointManager::connect_to(const std::string &ip, int port) {
  std::unique_ptr<PlasmaEndpoint> ep =
      std::make_unique<PlasmaEndpoint>(this, unreg_local_mr_);

  ep->active_connect(ip, port);
  add_connected_ep(ep->get_peer_ip(), ep.get());
  eps_.push_back(std::move(ep));
}

PlasmaEndpoint *EndpointManager::get_ep_to(const std::string &ip) {
  if (auto search = ips_to_connected_eps_.find(ip);
      search != ips_to_connected_eps_.end()) {
    return search->second;
  } else {
    return nullptr;
  }
}