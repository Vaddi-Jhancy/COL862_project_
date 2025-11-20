#ifndef COMMON_H
#define COMMON_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#pragma once
using namespace std;
using namespace std::chrono_literals;

struct Peer {
    string host;
    int port;
    Peer() {}
    Peer(string h, int p) : host(h), port(p) {}
};

inline void logf(const std::string &tag, const std::string &msg) {
  auto now = std::chrono::system_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch())
                .count();
  std::cout << "[" << ms << "][" << tag << "] " << msg << std::endl;
}

inline int tcp_listen(int port) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  int opt = 1;
  setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);
  if (bind(sock, (sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind");
    return -1;
  }
  if (listen(sock, 128) < 0) {
    perror("listen");
    return -1;
  }
  return sock;
}

inline int tcp_connect(const std::string &host, int port) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return -1;
  sockaddr_in serv;
  serv.sin_family = AF_INET;
  serv.sin_port = htons(port);
  serv.sin_addr.s_addr = inet_addr(host.c_str());
  if (connect(sock, (sockaddr *)&serv, sizeof(serv)) < 0) {
    close(sock);
    return -1;
  }
  return sock;
}

inline bool send_line(int sock, const std::string &line) {
  std::string s = line + "\n";
  const char *ptr = s.c_str();
  ssize_t left = s.size();
  while (left > 0) {
    ssize_t w = send(sock, ptr, left, 0);
    if (w <= 0) return false;
    left -= w;
    ptr += w;
  }
  return true;
}

inline bool recv_line(int sock, std::string &out) {
  out.clear();
  char c;
  while (true) {
    ssize_t r = recv(sock, &c, 1, 0);
    if (r <= 0) return false;
    if (c == '\n') break;
    out.push_back(c);
  }
  return true;
}

// parse pipe-delimited fields
inline std::vector<std::string> split_msg(const std::string &s) {
  std::vector<std::string> parts;
  std::string cur;
  for (char c : s) {
    if (c == '|') {
      parts.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  parts.push_back(cur);
  return parts;
}

#endif // COMMON_H
