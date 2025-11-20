#include <cstring>
#include <string>
#include <vector>
#include <iostream>

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cerr << "Usage: ./lazylog <role> ...\nRoles: sequencer shard client\n";
    return 1;
  }
  std::string role = argv[1];
  if (role == "sequencer") {
    extern int main_sequencer(int, char**);
    return main_sequencer(argc, argv);
  } else if (role == "shard") {
    extern int main_shard(int, char**);
    return main_shard(argc, argv);
  } else if (role == "client") {
    extern int main_client(int, char**);
    return main_client(argc, argv);
  } else {
    std::cerr << "Unknown role: " << role << "\n";
    return 1;
  }
}
