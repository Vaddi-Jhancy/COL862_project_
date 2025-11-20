#include "common.h"

// sequencer:
// - store APPENDs locally (record_id -> payload)
// - ACK clients on append
// - leader: dynamic batching (BATCH_MS), assign gp, send PUT to shards
// - when shard ACKs a pos mark durable; compute contiguous last_ordered_gp; stable_gp = last_ordered_gp
// - broadcast STABLE_UPDATE|stable and wait for STABLE_ACKs from replicas
// - after STABLE_ACKs from all replicas, send UPDATESTABLE|stable to shards and GC

#include <random>
#include <atomic>
#include <algorithm>
#include <unordered_map>
#include <set>

// ZooKeeper
#include <zookeeper/zookeeper.h>

int random_ms(int min_v, int max_v) {
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> dist(min_v, max_v);
    return dist(gen);
}

int REP_ID = 1;
static int PORT = 5001;
std::vector<Peer> PEERS; // other sequencer addresses
std::vector<Peer> SHARDS;

static std::mutex mu;
std::vector<std::string> pending_order; // record_id order
std::unordered_map<std::string,std::string> pending_payload; // record_id -> payload

// per-position durable tracking
std::mutex durable_mu;
std::set<uint64_t> durable_positions; // positions acked by shard

uint64_t last_ordered_gp = 0; // contiguous last ordered durable (leader-side)
static std::atomic<uint64_t> stable_gp{0};
std::atomic<uint64_t> next_gp{1};
std::atomic<bool> is_leader{false};
std::atomic<uint64_t> view_id{1};
std::atomic<bool> sealed{false};

std::mutex hb_mu;
std::chrono::steady_clock::time_point last_hb_recv;

const int HB_INTERVAL_MS = 200;
const int HB_TIMEOUT_MS = 700;
const int ORDER_PERIOD_MS = 20; // internal loop spin
const int BATCH_MS = 5; // dynamic batching window in ms

int majority_count() {
    int N = PEERS.size() + 1;
    return (N/2) + 1;
}

void send_to_peer(const Peer &p, const std::string &msg) {
  int s = tcp_connect(p.host, p.port);
  if (s < 0) return;
  send_line(s, msg);
  close(s);
}

// ZooKeeper globals
static zhandle_t *zk_handle = nullptr;
static std::string zk_connect_str = "127.0.0.1:2181";
static std::string election_root = "/lazylog/election";
static std::string my_znode_path;
static std::mutex zk_mu;

void zk_watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            logf("ZK", "connected");
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            logf("ZK", "session expired");
        }
    }
}

int ensure_path(const std::string &path) {
    int rc = zoo_exists(zk_handle, path.c_str(), 0, nullptr);
    if (rc == ZNONODE) {
        rc = zoo_create(zk_handle, path.c_str(), "", 0,
                        &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
        if (rc != ZOK && rc != ZNODEEXISTS) return rc;
    }
    return ZOK;
}

void zk_connect_and_participate() {
    zk_handle = zookeeper_init(zk_connect_str.c_str(), zk_watcher, 30000, 0, nullptr, 0);
    if (!zk_handle) {
        logf("ZK", "zookeeper_init failed");
        return;
    }
    ensure_path("/lazylog");
    ensure_path(election_root);

    char path_buf[256];
    int rc = zoo_create(
        zk_handle,
        (election_root + "/n_").c_str(),
        "", 0,
        &ZOO_OPEN_ACL_UNSAFE,
        ZOO_EPHEMERAL | ZOO_SEQUENCE,
        path_buf, sizeof(path_buf) - 1);
    if (rc != ZOK) {
        logf("ZK", "zoo_create election node failed rc=" + std::to_string(rc));
        return;
    }
    my_znode_path = path_buf;
    logf("ZK", "created election znode " + my_znode_path);
}

void zk_election_loop() {
    while (true) {
        if (!zk_handle || my_znode_path.empty()) {
            std::this_thread::sleep_for(1000ms);
            continue;
        }

        struct String_vector children;
        int rc = zoo_get_children(zk_handle, election_root.c_str(), 0, &children);
        if (rc != ZOK) {
            logf("ZK", "zoo_get_children failed rc=" + std::to_string(rc));
            std::this_thread::sleep_for(1000ms);
            continue;
        }

        std::vector<std::string> nodes;
        for (int i = 0; i < children.count; ++i) {
            nodes.emplace_back(children.data[i]);
        }
        deallocate_String_vector(&children);
        std::sort(nodes.begin(), nodes.end());

        std::string my_name = my_znode_path.substr(my_znode_path.find_last_of('/') + 1);

        bool am_leader = (!nodes.empty() && nodes[0] == my_name);

        if (am_leader && !is_leader.load()) {
            logf("ZK", "BECOME_LEADER via ZooKeeper, node=" + my_name);
            is_leader.store(true);
            sealed.store(false);
        } else if (!am_leader && is_leader.load()) {
            logf("ZK", "STEP_DOWN via ZooKeeper, node=" + my_name);
            is_leader.store(false);
            sealed.store(true);
        }

        std::this_thread::sleep_for(500ms);
    }
}

void handle_connection(int fd) {
  std::string line;
  if (!recv_line(fd, line)) { close(fd); return; }
  auto parts = split_msg(line);
  if (parts.empty()) { close(fd); return; }
  std::string cmd = parts[0];
  if (cmd == "APPEND") {
    // APPEND|record_id|clientid|payload
    std::string record_id = parts.size()>1?parts[1]:"";
    std::string payload = parts.size()>3?parts[3]:(parts.size()>2?parts[2]:"");
    {
      std::lock_guard lk(mu);
      if (!sealed.load()) {
        pending_order.push_back(record_id);
        pending_payload[record_id] = payload;
      } else {
        send_line(fd, "RETRY");
        close(fd); return;
      }
    }
    send_line(fd, "ACK");
  } else if (cmd == "HB") {
    uint64_t their_view = stoull(parts.size()>1?parts[1]:"0");
    uint64_t their_last = stoull(parts.size()>3?parts[3]:"0");
    if (their_view >= view_id.load()) view_id = their_view;
    {
      std::lock_guard lk(hb_mu);
      last_hb_recv = std::chrono::steady_clock::now();
    }
    if (their_last > last_ordered_gp) last_ordered_gp = their_last;
    send_line(fd, "HB_ACK");
  } else if (cmd == "HBQ") {
    std::string rep = "HB_REPLY|" + std::to_string(view_id.load()) + "|" + (is_leader.load() ? "1":"0") + "|" + std::to_string(last_ordered_gp) + "|" + std::to_string(stable_gp.load());
    send_line(fd, rep);
  } else if (cmd == "STABLE_UPDATE") {
    uint64_t s = stoull(parts.size()>1?parts[1]:"0");
    last_ordered_gp = s;
    stable_gp.store(s);
    send_line(fd, "STABLE_ACK");
  } else if (cmd == "GC") {
    uint64_t upto = stoull(parts.size()>1?parts[1]:"0");
    {
      std::lock_guard lk(mu);
      // demo: nothing strict
    }
    send_line(fd, "GC_OK");
  } else {
    send_line(fd, "ERR|UNKNOWN");
  }
  close(fd);
}

// send a PUT to a shard; returns true if shard replied PUT_OK
bool send_put_to_shard(const Peer &sh, uint64_t pos, const std::string &rid, const std::string &payload) {
  int s = tcp_connect(sh.host, sh.port);
  if (s < 0) return false;
  std::string msg = "PUT|" + std::to_string(pos) + "|" + rid + "|" + payload;
  send_line(s, msg);
  std::string rep;
  bool ok = false;
  if (recv_line(s, rep)) {
    if (rep == "PUT_OK") ok = true;
  }
  close(s);
  return ok;
}

void notify_shards_updatestable(uint64_t upto) {
  for (auto &sh: SHARDS) {
    int s = tcp_connect(sh.host, sh.port);
    if (s < 0) continue;
    send_line(s, std::string("UPDATESTABLE|") + std::to_string(upto));
    std::string rep;
    recv_line(s, rep);
    close(s);
  }
}

void broadcast_stable_and_wait(uint64_t upto) {
  for (auto &p : PEERS) {
    int s = tcp_connect(p.host, p.port);
    if (s < 0) continue;
    send_line(s, std::string("STABLE_UPDATE|") + std::to_string(upto));
    std::string rep;
    if (recv_line(s, rep)) {
      // expect STABLE_ACK
    }
    close(s);
  }
  stable_gp.store(upto);
  notify_shards_updatestable(upto);
}

void leader_ordering_loop() {
  logf("SEQ", "leader ordering loop started (batch window " + std::to_string(BATCH_MS) + "ms)");
  while (is_leader.load()) {
    auto batch_start = std::chrono::steady_clock::now();
    std::vector<std::string> batch_rids;
    {
      std::lock_guard lk(mu);
      if (!pending_order.empty()) {
        batch_rids = pending_order;
        pending_order.clear();
      }
    }
    if (batch_rids.empty()) {
      std::this_thread::sleep_for(ORDER_PERIOD_MS * 1ms);
      continue;
    }
    std::vector<std::pair<uint64_t,std::string>> assigned; // (pos,rid)
    for (auto &rid : batch_rids) {
      uint64_t pos = next_gp.fetch_add(1);
      assigned.push_back({pos, rid});
    }
    for (auto &pr : assigned) {
      uint64_t pos = pr.first;
      std::string rid = pr.second;
      std::string payload;
      {
        std::lock_guard lk(mu);
        auto it = pending_payload.find(rid);
        if (it != pending_payload.end()) {
          payload = it->second;
          pending_payload.erase(it);
        } else {
          payload = "";
        }
      }
      int shard_id = pos % SHARDS.size();
      bool ok = send_put_to_shard(SHARDS[shard_id], pos, rid, payload);
      if (ok) {
        std::lock_guard lk(durable_mu);
        durable_positions.insert(pos);
      } else {
        logf("SEQ", "failed to deliver pos " + std::to_string(pos) + " to shard " + std::to_string(shard_id));
      }
    }
    uint64_t new_last = last_ordered_gp;
    {
      std::lock_guard lk(durable_mu);
      uint64_t cur = last_ordered_gp;
      while (durable_positions.count(cur + 1)) {
        cur++;
      }
      if (cur > last_ordered_gp) {
        new_last = cur;
        last_ordered_gp = cur;
        for (auto it = durable_positions.begin(); it != durable_positions.end(); ) {
          if (*it <= cur) it = durable_positions.erase(it);
          else ++it;
        }
      }
    }
    if (new_last > stable_gp.load()) {
      broadcast_stable_and_wait(new_last);
    }
    std::this_thread::sleep_for(10ms);
  }
  logf("SEQ", "leader ordering loop ended");
}

void leader_hb_loop() {
  while (is_leader.load()) {
    std::string msg = "HB|" + std::to_string(view_id.load()) + "|" + std::to_string(REP_ID) + "|" + std::to_string(last_ordered_gp);
    for (auto &p : PEERS) {
      int s = tcp_connect(p.host, p.port);
      if (s < 0) continue;
      send_line(s, msg);
      std::string rep;
      recv_line(s, rep);
      close(s);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(random_ms(100,300)));
  }
}

static void serve_loop() {
  int listen_fd = tcp_listen(PORT);
  if (listen_fd < 0) { perror("listen"); return; }
  logf("SEQ", "listening on port " + std::to_string(PORT));
  while (true) {
    int fd = accept(listen_fd, nullptr, nullptr);
    if (fd < 0) continue;
    std::thread t(handle_connection, fd);
    t.detach();
  }
}

int main_sequencer(int argc, char **argv) {
  if (argc < 9) {
    std::cerr << "Usage: sequencer <id> <port> <peer1>:<port> <peer2>:<port> <shard1>:<port> <shard2>:<port> <shard3>:<port>\n";
    return 1;
  }
  REP_ID = atoi(argv[2]);
  PORT = atoi(argv[3]);
  for (int i = 4; i < 6; ++i) {
    std::string s = argv[i]; auto pos = s.find(':');
    if (pos == std::string::npos) continue;
    PEERS.push_back({s.substr(0,pos), stoi(s.substr(pos+1))});
  }
  for (int i = 6; i < 9; ++i) {
    std::string s = argv[i]; auto pos = s.find(':');
    if (pos == std::string::npos) continue;
    SHARDS.push_back({s.substr(0,pos), stoi(s.substr(pos+1))});
  }
  last_hb_recv = std::chrono::steady_clock::now();
  std::thread server(serve_loop);
  server.detach();

  // ZooKeeper-based leader election
  zk_connect_and_participate();
  std::thread zk_el(zk_election_loop);
  zk_el.detach();

  is_leader = false;
  sealed = true; // until ZooKeeper says we are leader

  while (true) {
    if (is_leader.load()) {
      std::thread ord(leader_ordering_loop); ord.detach();
      std::thread hb(leader_hb_loop); hb.detach();
      while (is_leader.load()) std::this_thread::sleep_for(500ms);
    } else {
      std::this_thread::sleep_for(200ms);
    }
  }
  return 0;
}
