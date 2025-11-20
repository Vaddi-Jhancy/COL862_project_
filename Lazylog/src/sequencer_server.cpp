#include "sequencer_server.h"
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>
#include <string>
#include <grpcpp/grpcpp.h>
#include "sequencer.h"
#include <zookeeper/zookeeper.h>
#include <chrono>
#include "generated/sequencer.grpc.pb.h"
#include "generated/sequencer.pb.h"
#include "generated/sequencer_internal.grpc.pb.h"

#include <signal.h>

static Sequencer* GLOBAL_SEQ_PTR = nullptr;

void handle_seal_signal(int) {
    if (GLOBAL_SEQ_PTR) {
        GLOBAL_SEQ_PTR->seal_view();
    }
}


using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using sequencer::SequencerService;
using sequencer::AppendRequest;
using sequencer::AppendReply;

using sequencer_internal::SequencerInternal;
using sequencer_internal::ReplicateAppendRequest;
using sequencer_internal::ReplicateAppendReply;

// Implementation of client-facing Append RPC (leader only; followers reject)
// Implementation of client-facing Append RPC (leader only; followers reject)
class SequencerServiceImpl final : public SequencerService::Service {
public:
    // keep only reference to Sequencer (no copied flag)
    SequencerServiceImpl(Sequencer &s) : seq_(s) {}

    Status Append(ServerContext* context, const AppendRequest* req,
                  AppendReply* reply) override {

        // Reject if sealed
        if (seq_.sealed.load()) {
            reply->set_success(false);
            reply->set_global_pos(-1);
            reply->set_message("View is sealed");
            return Status::OK;
        }

        // Use live state from Sequencer (not a copied bool)
        if (!seq_.is_leader.load()) {
            reply->set_success(false);
            reply->set_global_pos(-1);
            reply->set_message("Not leader");
            return Status::OK;
        }

        // 1) append locally
        int local_idx = seq_.append_local_entry(req->client_id(), req->req_id(), req->record());

        // 2) replicate to followers
        bool repl_ok = seq_.replicate_to_followers(local_idx);

        if (!repl_ok) {
            reply->set_success(false);
            reply->set_global_pos(-1);
            reply->set_message("Replication failed");
            return Status::OK;
        }

        // 3) assign global position
        int64_t gp = seq_.assign_global_pos(local_idx);

        reply->set_success(true);
        reply->set_global_pos(gp);
        reply->set_message("Appended and replicated");
        return Status::OK;
    }

private:
    Sequencer &seq_;
};

// Implementation of internal service that followers expose
class SequencerInternalImpl final : public SequencerInternal::Service {
public:
    SequencerInternalImpl(Sequencer &s) : seq_(s) {}

    Status ReplicateAppend(ServerContext* context, const ReplicateAppendRequest* req,
                           ReplicateAppendReply* reply) override {
        // Follower: append to local log and ack
        int local_idx = seq_.append_local_entry(req->client_id(), req->req_id(), req->record());
        std::cout << "[FOLLOWER] Received ReplicateAppend local_idx=" << local_idx << "\n";
        reply->set_ok(true);
        reply->set_message("OK");
        return Status::OK;
    }

private:
    Sequencer &seq_;
};

// Utility: parse followers string "a:b,c:d"
static std::vector<std::string> parse_followers(const std::string &s) {
    std::vector<std::string> res;
    std::stringstream ss(s);
    while (ss.good()) {
        std::string token;
        std::getline(ss, token, ',');
        if (!token.empty()) res.push_back(token);
    }
    return res;
}

// Connect to ZooKeeper and create an ephemeral znode for this replica.
// The znode stays alive as long as this process stays connected.
zhandle_t* zk_register_replica(const std::string& zk_addr,
                               const std::string& znode_path,
                               const std::string& data)
{
    int timeout_ms = 30000;
    zhandle_t* zh = zookeeper_init(zk_addr.c_str(), nullptr, timeout_ms, 0, nullptr, 0);
    if (!zh) {
        std::cerr << "[ZK] ERROR: Could not connect to ZooKeeper at " << zk_addr << "\n";
        return nullptr;
    }

    // Retry create (session may take a short time to become CONNECTED)
    const int MAX_ATTEMPTS = 5;
    int rc = ZCONNECTIONLOSS;
    for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
        rc = zoo_create(zh,
                        znode_path.c_str(),
                        data.c_str(),
                        data.size(),
                        &ZOO_OPEN_ACL_UNSAFE,
                        ZOO_EPHEMERAL,
                        nullptr,
                        0);
        if (rc == ZOK) {
            std::cout << "[ZK] Registered replica at " << znode_path << "\n";
            break;
        }
        if (rc == ZNODEEXISTS) {
            // for a quick demo: try delete and recreate once
            std::cerr << "[ZK] Warning: znode already exists at " << znode_path
                      << " (attempt " << attempt << "), trying delete+recreate\n";
            int d = zoo_delete(zh, znode_path.c_str(), -1);
            if (d != ZOK) {
                std::cerr << "[ZK] Warning: failed to delete existing znode rc=" << d << "\n";
                // fallthrough to retry which may succeed later
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue; // try create again immediately
            }
        } else {
            std::cerr << "[ZK] create attempt " << attempt << " failed rc=" << rc << "\n";
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    if (rc != ZOK) {
        std::cerr << "[ZK] ERROR: Failed to create ephemeral znode " << znode_path
                  << " rc=" << rc << "\n";
    }

    return zh;   // keep the handle alive!
}


// ------- ZK election helpers -------

// Create ephemeral sequential znode under base_path (e.g. "/lazylog/election")
// returns full created path on success, empty string on failure.
// out_node_name will contain just the last path component (e.g. "node-0000000003")
static std::string zk_create_ephemeral_sequential(zhandle_t* zh,
                                                  const std::string &base_path,
                                                  const std::string &data,
                                                  std::string &out_node_name)
{
    // create path base like "/lazylog/election/node-"
    std::string prefix = base_path;
    if (prefix.back() == '/') prefix.pop_back();
    prefix += "/node-";

    char created_path[512];
    int buf_len = sizeof(created_path);
    int rc = zoo_create(zh,
                        prefix.c_str(),
                        data.c_str(),
                        data.size(),
                        &ZOO_OPEN_ACL_UNSAFE,
                        ZOO_EPHEMERAL | ZOO_SEQUENCE,
                        created_path,
                        buf_len);
    if (rc != ZOK) {
        std::cerr << "[ZK] ERROR: create sequential node failed rc=" << rc << "\n";
        return "";
    }
    std::string full(created_path);
    // extract last component
    size_t pos = full.find_last_of('/');
    if (pos == std::string::npos) out_node_name = full;
    else out_node_name = full.substr(pos+1);
    std::cout << "[ZK] Created election node " << full << "\n";
    return full;
}

// List children of a path into a vector<string>. Returns true on success.
static bool zk_list_children(zhandle_t* zh, const std::string &path, std::vector<std::string> &out_children)
{
    struct String_vector sv;
    int rc = zoo_get_children(zh, path.c_str(), 0, &sv);
    if (rc != ZOK) {
        std::cerr << "[ZK] ERROR: get_children " << path << " rc=" << rc << "\n";
        return false;
    }
    out_children.clear();
    for (int i = 0; i < sv.count; ++i) {
        out_children.emplace_back(sv.data[i]);
    }
    deallocate_String_vector(&sv); // free memory allocated by ZK client
    return true;
}

// Helper that returns numeric suffix of a sequential node like "node-0000000003" -> 3.
// If parse fails returns a large value.
static unsigned long parse_seq_suffix(const std::string &name) {
    // find last '-' then parse digits
    size_t pos = name.find_last_of('-');
    if (pos == std::string::npos) return ULONG_MAX;
    std::string suf = name.substr(pos + 1);
    // trim leading non-digits just in case
    size_t first_digit = suf.find_first_of("0123456789");
    if (first_digit != std::string::npos) suf = suf.substr(first_digit);
    try {
        return std::stoul(suf);
    } catch (...) {
        return ULONG_MAX;
    }
}

// Election loop: create sequential znode and repeatedly check smallest child.
// If this node is smallest -> become leader, else follower.
// Runs until process exits.
static void election_loop(zhandle_t* zh, Sequencer* seq_ptr, const std::string &election_path, const std::string &my_node_name, int poll_ms=500) {
    if (!zh || !seq_ptr) return;

    std::string my_node = my_node_name; // e.g. node-0000000003

    while (true) {
        std::vector<std::string> children;
        bool ok = zk_list_children(zh, election_path, children);
        if (!ok) {
            std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
            continue;
        }

        // DEBUG: print children seen by this node
        std::cout << "[ELECTION][DEBUG] children under " << election_path << ":";
        for (auto &c : children) std::cout << " " << c;
        std::cout << "\n";

        // find smallest by numeric suffix
        unsigned long min_val = ULONG_MAX;
        std::string min_node;
        for (auto &c : children) {
            unsigned long v = parse_seq_suffix(c);
            if (v < min_val) {
                min_val = v;
                min_node = c;
            }
        }

        if (!min_node.empty() && min_node == my_node) {
            // I'm the leader
            if (!seq_ptr->is_leader.load()) {
                seq_ptr->become_leader();   // will unseal
                seq_ptr->unseal_view();
                std::cout << "[ELECTION] elected leader (node=" << my_node << ")\n";
            }
        } else {
            // I'm a follower
            if (seq_ptr->is_leader.load()) {
                // if I was leader earlier, step down
                seq_ptr->become_follower();
                seq_ptr->seal_view();
                std::cout << "[ELECTION] stepping down (node=" << my_node << ")\n";
            } else {
                // ensure follower is sealed
                seq_ptr->seal_view();
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
    }
}





void SequencerServer::Run(const std::string& role, int port, const std::vector<std::string>& followers) {
    std::string addr = "0.0.0.0:" + std::to_string(port);
    Sequencer seq;
    GLOBAL_SEQ_PTR = &seq;
    signal(SIGUSR1, handle_seal_signal);

    // follower list
    if (!followers.empty()) seq.followers = followers;
    bool is_leader = (role == "leader");   // only used for initial boot

    // -----------------------------------------
    // ---- ZooKeeper registration for replica ----
    // -----------------------------------------
    std::string zk_addr = "127.0.0.1:2181";
    std::string replica_path = "/lazylog/replicas/replica-" + std::to_string(port);
    std::string replica_data = "127.0.0.1:" + std::to_string(port);

    zhandle_t* zk_handle = zk_register_replica(zk_addr, replica_path, replica_data);
    if (!zk_handle) {
        std::cerr << "[ZK] WARNING: Replica registration failed (continuing without ZK)\n";
    }

    // -----------------------------------------
    // ---- Initial explicit role state ----
    // -----------------------------------------
    if (is_leader) {
        seq.become_leader();  // unseals
        std::cout << "[INIT] Node started as LEADER (temporary), view unsealed.\n";
    } else {
        seq.become_follower();
        seq.seal_view();
        std::cout << "[INIT] Node started as FOLLOWER, view sealed.\n";
    }

    // -----------------------------------------
    // ---- Leader Election Setup (ZK sequential) ----
    // -----------------------------------------
    if (zk_handle) {
        std::string election_path = "/lazylog/election";

        // Ensure election path exists (ignore error if exists)
        int mk_rc = zoo_create(zk_handle, election_path.c_str(), "",
                               0, &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);

        if (mk_rc != ZOK && mk_rc != ZNODEEXISTS) {
            std::cerr << "[ZK] Warning: Cannot create " << election_path << " rc=" << mk_rc << "\n";
        }

        // Create ephemeral sequential znode
        std::string my_node_name;
        std::string created_path =
            zk_create_ephemeral_sequential(zk_handle, election_path, replica_data, my_node_name);

        if (!created_path.empty()) {
            std::cout << "[ELECTION] My election node: " << created_path << "\n";
            std::cout << "[ELECTION][DEBUG] my_node_name (last component): " << my_node_name << "\n";

            // Start election loop in background
            std::thread(election_loop,
                        zk_handle,
                        &seq,
                        election_path,
                        my_node_name,
                        500      // poll every 500ms
            ).detach();
        } else {
            std::cerr << "[ELECTION] ERROR: Election node creation failed; no failover.\n";
        }
    } else {
        std::cerr << "[ELECTION] ZooKeeper handle null, skipping election setup.\n";
    }

    // -----------------------------------------
    // ---- Start gRPC server ----
    // -----------------------------------------
    SequencerServiceImpl service(seq);
    SequencerInternalImpl internal_service(seq);

    ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    builder.RegisterService(&internal_service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "[" << role << "] Server listening on " << addr << "\n";

    if (is_leader) {
        std::cout << "[LEADER] followers:";
        for (auto &f : seq.followers) std::cout << " " << f;
        std::cout << "\n";
    }

    server->Wait();

    // (Optional) Clean-up code could go here, e.g. zookeeper_close(zk_handle) etc.
}



