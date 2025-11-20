#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "sequencer.grpc.pb.h"
#include "sequencer.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using sequencer::SequencerService;
using sequencer::AppendRequest;
using sequencer::AppendReply;

class AppendClient {
public:
    AppendClient(std::shared_ptr<Channel> channel)
      : stub_(SequencerService::NewStub(channel)) {}

    void Append(int client_id, int req_id, const std::string &record) 
    {
        AppendRequest req;
        req.set_client_id(client_id);
        req.set_req_id(req_id);
        req.set_record(record);

        AppendReply reply;
        ClientContext ctx;

        Status st = stub_->Append(&ctx, req, &reply);

        if (st.ok()) {
            std::cout << "Append success=" << reply.success()
                      << " gp=" << reply.global_pos()
                      << " msg=" << reply.message() << "\n";
        } else {
            std::cerr << "RPC failed: " << st.error_message() << "\n";
        }
    }

private:
    std::unique_ptr<SequencerService::Stub> stub_;
};

int main(int argc, char** argv) 
{
    std::string server_addr = "127.0.0.1:50051";
    int client_id = 1;
    std::string record = "default_record";

    // Parse CLI flags
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];

        if (a.rfind("--server_addr=", 0) == 0) {
            server_addr = a.substr(14);      // OK
        } else if (a.rfind("--id=", 0) == 0) {
            client_id = std::stoi(a.substr(5));   // OK
        } else if (a.rfind("--record=", 0) == 0) {
            record = a.substr(9);
        }
    }

    AppendClient c(
        grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials())
    );

    // Single append call
    c.Append(client_id, 1, record);

    return 0;
}
