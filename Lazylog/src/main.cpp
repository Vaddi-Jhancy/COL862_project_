// #include <iostream>
// #include "sequencer_server.h"

// int main(int argc, char** argv) {
//     std::cout << "Starting Sequencer server..." << std::endl;

//     SequencerServer server;
//     server.Run();   // or whatever your server entry method is

//     return 0;
// }

#include "sequencer_server.h"
#include <iostream>
#include <vector>
#include <string>

int main(int argc, char** argv) {
    std::string role = "leader";
    int port = 50051;
    std::vector<std::string> followers;

    for (int i=1; i<argc; i++){
        std::string a = argv[i];
        if (a.rfind("--role=",0)==0) role = a.substr(7);
        if (a.rfind("--port=",0)==0) port = std::stoi(a.substr(7));
        if (a.rfind("--followers=",0)==0) {
            std::string followers_arg = a.substr(12);
            // parse followers into vector
            size_t start = 0, end;
            while ((end = followers_arg.find(',', start)) != std::string::npos) {
                followers.push_back(followers_arg.substr(start, end-start));
                start = end+1;
            }
            if (start < followers_arg.size())
                followers.push_back(followers_arg.substr(start));
        }
    }

    SequencerServer server;
    server.Run(role, port, followers);

    return 0;
}
