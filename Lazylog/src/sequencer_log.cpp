#include "sequencer_log.h"

int SequencerLog::append(const Entry& e) {
    log.push_back(e);
    last_local_index++;
    return last_local_index;
}

SequencerLog::Entry SequencerLog::get(int index) {
    return log[index];
}

void SequencerLog::gc_up_to(int index) {
    if(index >= 0 && index < log.size())
        log.erase(log.begin(), log.begin() + index + 1);
}
