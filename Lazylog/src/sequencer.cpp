#include "sequencer.h"
#include "sequencer_log.h"
#include <grpcpp/grpcpp.h>
#include "generated/sequencer_internal.grpc.pb.h"
#include <chrono>
#include <thread>
#include <iostream>
#include <algorithm>          // for std::max
#include <vector>

int Sequencer::append_local_entry(int client_id, int req_id, const std::string &record) {
    std::lock_guard<std::mutex> lk(mtx);
    int local_idx = state.log.append({client_id, req_id, record});
    std::cout << "[LOCAL] Appended local idx " << local_idx << "\n";
    std::cout << "[APPEND] client=" << client_id
              << " req=" << req_id
              << " local_idx=" << local_idx
              << " record=" << record << "\n";
    return local_idx;
}

/*
  Replicate to all follower addresses in followers vector.
  Simple synchronous unary RPC no batching for now.
*/
bool Sequencer::replicate_to_followers(int local_index) {
    // read entry
    SequencerLog::Entry e = state.log.get(local_index);

    // require at least zero followers -> that's okay (single node)
    if (followers.empty()) {
        std::cout << "[REPL] No followers configured. Treating as replicated locally.\n";
        return true;
    }

    std::cout << "[REPL] Replicating local_idx=" << local_index 
              << " to " << followers.size() << " followers\n";

    // For each follower, create stub and call ReplicateAppend
    int success_count = 0;
    for (const auto &addr : followers) {
        // Create channel and stub
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        auto stub = sequencer_internal::SequencerInternal::NewStub(channel);

        sequencer_internal::ReplicateAppendRequest req;
        req.set_client_id(e.client_id);
        req.set_req_id(e.req_id);
        req.set_record(e.record);
        req.set_local_index(local_index);

        sequencer_internal::ReplicateAppendReply reply;
        grpc::ClientContext ctx;
        // retry basic loop (2 tries)
        bool ok = false;
        for (int attempt=0; attempt<2 && !ok; ++attempt) {
            grpc::Status status = stub->ReplicateAppend(&ctx, req, &reply);
            if (status.ok() && reply.ok()) {
                ok = true;
            } else {
                std::cerr << "[REPL:" << addr << "] attempt " << attempt << " failed: "
                          << (status.ok() ? reply.message() : status.error_message()) << "\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        if (ok) success_count++;
    }

    bool all_ok = (success_count == (int)followers.size());
    std::cout << "[REPL] replication result: " << success_count << "/" << followers.size() << "\n";
    return all_ok;
}

int Sequencer::assign_global_pos(int local_index) {
    // Use atomic monotonic counter to avoid races and ensure strictly increasing gp
    int64_t gp = next_global_pos.fetch_add(1, std::memory_order_relaxed);

    std::cout << "[GLOBAL] Assigned GP=" << gp 
          << " for local_idx=" << local_index << "\n";


    // record mapping local_index -> gp
    {
        std::lock_guard<std::mutex> lk(mtx);
        local_to_gp[local_index] = gp;
        // update state last ordered / stable
        state.last_ordered_gp = gp;
        state.stable_gp = gp;
    }

    // For demo: shard is only for logging, not used to compute gp
    const int NUM_SHARDS = 2;
    int shard = (int)(gp % NUM_SHARDS);

    std::cout << "[ORDER] Assigned global_pos " << gp << " to local_index " << local_index
              << " (shard=" << shard << ")\n";
    return (int)gp;
}

void Sequencer::gc_up_to(int gp) {
    std::lock_guard<std::mutex> lk(mtx);

    // Remove mapping entries with gp' <= gp; find maximum local_index we can drop
    int max_local_to_gc = -1;
    std::vector<int> to_erase;
    for (auto &kv : local_to_gp) {
        int local_idx = kv.first;
        int64_t mapped_gp = kv.second;
        if (mapped_gp <= gp) {
            max_local_to_gc = std::max(max_local_to_gc, local_idx);
            to_erase.push_back(local_idx);
        }
    }

    // erase mapping entries
    for (int li : to_erase) {
        local_to_gp.erase(li);
    }

    if (max_local_to_gc >= 0) {
        // GC local log up to the computed local index
        state.log.gc_up_to(max_local_to_gc);
        state.stable_gp = gp;
        std::cout << "[GC] GC done up to gp " << gp << " (local_index " << max_local_to_gc << ")\n";
    } else {
        std::cout << "[GC] Nothing to GC for gp " << gp << "\n";
    }
}
