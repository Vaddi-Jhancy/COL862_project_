#include "common.h"

// shard:
// - receives PUT|pos|rid|payload and stores pos->{rid,payload}
// - maintains stable_gp (atomic)
// - on READRANGE|from|to: if stable_gp < to -> query leader via HBQ for stable; if leader stable >= to -> retry once
// - returns BATCHVAL|n|pos|rid|payload|...

#include <atomic>
#include <vector>
#include <unordered_map>

int SHARD_ID;
static int PORT;
static std::vector<Peer> SEQS;

static std::mutex mu;
std::map<uint64_t, std::pair<std::string,std::string>> store; // pos -> (rid,payload)
uint64_t max_pos = 0;
static std::atomic<uint64_t> stable_gp{0};

std::string query_sequencer_hb(const Peer &p) {
  int s = tcp_connect(p.host, p.port);
  if (s < 0) return "";
  send_line(s, "HBQ");
  std::string rep;
  if (!recv_line(s, rep)) { close(s); return ""; }
  close(s);
  return rep;
}

void handle_conn(int fd) {
  std::string line;
  if (!recv_line(fd, line)) { close(fd); return; }
  auto parts = split_msg(line);
  if (parts.empty()) { close(fd); return; }
  std::string cmd = parts[0];
  if (cmd == "PUT") {
    uint64_t pos = stoull(parts.size()>1?parts[1]:"0");
    std::string rid = parts.size()>2?parts[2]:"";
    std::string payload = parts.size()>3?parts[3]:"";
    {
      std::lock_guard lk(mu);
      store[pos] = {rid, payload};
      if (pos > max_pos) max_pos = pos;
    }
    send_line(fd, "PUT_OK");
  } else if (cmd == "UPDATESTABLE") {
    uint64_t p = stoull(parts.size()>1?parts[1]:"0");
    stable_gp.store(std::max<uint64_t>(stable_gp.load(), p));
    send_line(fd, "OK");
  } else if (cmd == "READRANGE") {
    uint64_t fromp = stoull(parts.size()>1?parts[1]:"0");
    uint64_t top = stoull(parts.size()>2?parts[2]:"0");
    uint64_t cur_stable = stable_gp.load();
    if (cur_stable < top) {
      uint64_t best_stable = cur_stable;
      for (auto &sq : SEQS) {
        std::string rep = query_sequencer_hb(sq);
        if (rep.empty()) continue;
        auto pparts = split_msg(rep);
        if (pparts.size() >= 5 && pparts[0] == "HB_REPLY") {
          int is_lead = stoi(pparts[2]);
          uint64_t leader_stable = stoull(pparts[4]);
          if (is_lead) { best_stable = std::max<uint64_t>(best_stable, leader_stable); break; }
          best_stable = std::max<uint64_t>(best_stable, leader_stable);
        }
      }
      if (best_stable > cur_stable) stable_gp.store(best_stable);
      cur_stable = stable_gp.load();
    }
    if (cur_stable < top) {
      send_line(fd, "NOT_READY");
      close(fd); return;
    }
    std::vector<std::tuple<uint64_t,std::string,std::string>> results;
    {
      std::lock_guard lk(mu);
      for (uint64_t p = fromp; p <= top; ++p) {
        if (store.count(p)) {
          auto &pr = store[p];
          results.push_back({p, pr.first, pr.second});
        }
      }
    }
    std::ostringstream ss;
    ss << "BATCHVAL|" << results.size();
    for (auto &t : results) {
      uint64_t pos; std::string rid, payload;
      std::tie(pos, rid, payload) = t;
      ss << "|" << pos << "|" << rid << "|" << payload;
    }
    send_line(fd, ss.str());
  } else {
    send_line(fd, "ERR");
  }
  close(fd);
}

static void serve_loop() {
  int l = tcp_listen(PORT);
  if (l < 0) { perror("shard listen"); return; }
  logf("SHARD", "listening on port " + std::to_string(PORT));
  while (true) {
    int fd = accept(l, nullptr, nullptr);
    if (fd < 0) continue;
    std::thread t(handle_conn, fd);
    t.detach();
  }
}

int main_shard(int argc, char **argv) {
  if (argc < 6) { std::cerr << "Usage: shard <id> <port> <seq1> <seq2> <seq3>\n"; return 1; }
  SHARD_ID = atoi(argv[2]);
  PORT = atoi(argv[3]);
  for (int i = 4; i < argc; ++i) {
    std::string s = argv[i]; auto pos = s.find(':');
    if (pos == std::string::npos) continue;
    SEQS.push_back({s.substr(0,pos), stoi(s.substr(pos+1))});
  }
  serve_loop();
  return 0;
}
