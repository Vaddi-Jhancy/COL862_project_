#pragma once
#include <vector>
#include <string>

class SequencerLog {
public:
    struct Entry {
        int client_id;
        int req_id;
        std::string record;
    };

private:
    std::vector<Entry> log;
    int64_t last_local_index = -1;

public:
    int append(const Entry& e);
    Entry get(int index);
    void gc_up_to(int index);
    int size() { return log.size(); }
};
