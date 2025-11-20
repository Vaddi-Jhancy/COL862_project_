#include "common.h"

// Usage: client <id> <seq1> <seq2> <seq3> <shard1> <shard2> <shard3>
// Client: append -> send to all sequencers until all ACK
// readrange -> send to all shards, gather responses and assemble results

int CID = 1;
static std::vector<Peer> SEQS;
static std::vector<Peer> SHARDS;

uint64_t local_counter = 0;

std::string make_record_id() {
  auto now = std::chrono::system_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  uint64_t c = __sync_add_and_fetch(&local_counter, 1);
  std::ostringstream ss;
  ss << CID << "-" << ms << "-" << c;
  return ss.str();
}

bool append_to_all_until_acked(const std::string &record_id, const std::string &payload) {
  std::string msg = "APPEND|" + record_id + "|" + std::to_string(CID) + "|" + payload;
  int total = SEQS.size();
  std::vector<bool> acked(total, false);
  while (true) {
    for (size_t i = 0; i < SEQS.size(); ++i) {
      if (acked[i]) continue;
      auto &p = SEQS[i];
      int s = tcp_connect(p.host, p.port);
      if (s < 0) continue;
      send_line(s, msg);
      std::string rep;
      if (recv_line(s, rep)) {
        if (rep == "ACK") acked[i] = true;
        else if (rep == "RETRY") {
          // just retry later
        }
      }
      close(s);
    }
    int acks = 0;
    for (bool b : acked) if (b) acks++;
    if (acks == total) return true;
    std::this_thread::sleep_for(50ms);
  }
  return false;
}

// parse BATCHVAL response from shard: BATCHVAL|n|pos|rid|payload|pos|rid|payload...
std::vector<std::tuple<uint64_t,std::string,std::string>> parse_batchval(const std::string &s) {
  std::vector<std::tuple<uint64_t,std::string,std::string>> out;
  auto parts = split_msg(s);
  if (parts.size() < 2) return out;
  if (parts[0] != "BATCHVAL") return out;
  int n = stoi(parts[1]);
  size_t idx = 2;
  for (int i = 0; i < n; ++i) {
    if (idx + 2 >= parts.size()) break;
    uint64_t pos = stoull(parts[idx++]);
    std::string rid = parts[idx++];
    std::string payload = parts[idx++];
    out.push_back({pos, rid, payload});
  }
  return out;
}

// readrange: ask all shards and assemble results
void read_range(uint64_t from_pos, uint64_t to_pos) {
  std::map<uint64_t, std::pair<std::string,std::string>> results; // pos -> (rid,payload)
  for (auto &sh : SHARDS) {
    int s = tcp_connect(sh.host, sh.port);
    if (s < 0) continue;
    std::string req = "READRANGE|" + std::to_string(from_pos) + "|" + std::to_string(to_pos);
    send_line(s, req);
    std::string rep;
    if (!recv_line(s, rep)) { close(s); continue; }
    close(s);
    if (rep == "NOT_READY") {
      std::cout << "Shard " << sh.host << ":" << sh.port << " NOT_READY for range\n";
      continue;
    }
    auto items = parse_batchval(rep);
    for (auto &t : items) {
      uint64_t pos; std::string rid, payload;
      std::tie(pos, rid, payload) = t;
      results[pos] = {rid, payload};
    }
  }
  // print assembled results in order
  for (uint64_t p = from_pos; p <= to_pos; ++p) {
    if (results.count(p)) {
      auto &pr = results[p];
      std::cout << p << " | " << pr.first << " | " << pr.second << "\n";
    } else {
      std::cout << p << " | MISSING\n";
    }
  }
}

int main_client(int argc, char **argv) {
  if (argc < 8) { std::cerr << "Usage: client <id> <seq1> <seq2> <seq3> <shard1> <shard2> <shard3>\n"; return 1; }
  CID = atoi(argv[1]);
  for (int i = 2; i < 5; ++i) {
    std::string s = argv[i]; auto pos = s.find(':');
    SEQS.push_back({s.substr(0,pos), stoi(s.substr(pos+1))});
  }
  for (int i = 5; i < 8; ++i) {
    std::string s = argv[i]; auto pos = s.find(':');
    SHARDS.push_back({s.substr(0,pos), stoi(s.substr(pos+1))});
  }

  std::cout << "Client interactive. Commands:\n  append <text>\n  readrange <from> <to>\n  quit\n";
  std::string cmd;
  while (true) {
    std::cout << "> ";
    if (!std::getline(std::cin, cmd)) break;
    if (cmd.empty()) continue;
    if (cmd == "quit") break;
    if (cmd.rfind("append ",0) == 0) {
      std::string payload = cmd.substr(7);
      std::string rid = make_record_id();
      bool ok = append_to_all_until_acked(rid, payload);
      if (ok) std::cout << "Append OK record_id=" << rid << "\n";
      else std::cout << "Append FAILED\n";
    } else if (cmd.rfind("readrange ",0) == 0) {
      std::istringstream iss(cmd.substr(10));
      uint64_t a,b; if (!(iss >> a >> b)) { std::cout << "bad args\n"; continue; }
      read_range(a,b);
    } else {
      std::cout << "Unknown command\n";
    }
  }
  return 0;
}
