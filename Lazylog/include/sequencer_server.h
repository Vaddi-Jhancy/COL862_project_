#ifndef SEQUENCER_SERVER_H
#define SEQUENCER_SERVER_H

#include <string>
#include <vector>   
#include "sequencer.h"
#include "sequencer_internal.grpc.pb.h"
#include "sequencer.grpc.pb.h"


class SequencerServer {
public:
    void Run(const std::string& role, int port, const std::vector<std::string>& followers);
};


#endif
