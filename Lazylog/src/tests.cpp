#include "sequencer_log.h"
#include <iostream>
int main() {
    SequencerLog log;
    for(int i=0;i<1000;i++){
        log.append({1,i,"record"});
    }
    std::cout << "Size after append: " << log.size() << "\n"; // 1000
    log.gc_up_to(499);
    std::cout << "Size after GC 500: " << log.size() << "\n"; // 500
}
