#pragma once
#include "sequencer_log.h"

struct SequencerState {
    int64_t last_ordered_gp = -1;
    int64_t stable_gp = -1; // leader only
    int view = 0;
    bool is_leader = false;
    SequencerLog log;
};
