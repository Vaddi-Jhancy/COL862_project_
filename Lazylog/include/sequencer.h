#pragma once
#include "sequencer_state.h"
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <atomic>
#include <unordered_map>
#include <iostream>



class Sequencer {
public:
    SequencerState state;

    // follower addresses e.g. {"127.0.0.1:50052", "127.0.0.1:50053"}
    std::vector<std::string> followers;

    // concurrency
    std::mutex mtx;

    // monotonic global position counter
    std::atomic<int64_t> next_global_pos {0};

    // mapping from local_index -> global_pos
    std::unordered_map<int, int64_t> local_to_gp;

    Sequencer() = default;

    // append locally, returns local_index
    int append_local_entry(int client_id, int req_id, const std::string &record);

    // replicate to followers synchronously (waits for all acks)
    bool replicate_to_followers(int local_index);

    // called by leader when replication succeeded to assign global position
    int assign_global_pos(int local_index);

    // perform GC locally up to gp (global positon)
    void gc_up_to(int gp);

    std::atomic<bool> sealed{false};

    void seal_view() {
        sealed.store(true);
        std::cout << "[SEQUENCER] View sealed, rejecting new appends.\n";
    }

    void unseal_view() {
        sealed.store(false);
        std::cout << "[SEQUENCER] View unsealed, accepting appends.\n";
    }

    // --------------------------
    // Role state
    // --------------------------
    std::atomic<bool> is_leader{false};

    void become_leader() {
        is_leader.store(true);
        sealed.store(false);
        std::cout << "[ELECTION] This node became LEADER.\n";
    }

    void become_follower() {
        is_leader.store(false);
        // keep sealed state unchanged
        std::cout << "[ELECTION] This node is FOLLOWER.\n";
    }




};
