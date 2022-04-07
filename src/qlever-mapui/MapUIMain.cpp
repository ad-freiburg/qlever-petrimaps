// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Patrick Brosi <brosi@informatik.uni-freiburg.de>

#include <iostream>
#include "qlever-mapui/server/Server.h"
#include "util/Misc.h"
#include "util/http/Server.h"
#include "util/log/Log.h"

using mapui::Server;

// _____________________________________________________________________________
void printHelp(int argc, char** argv) {
  UNUSED(argc);
  std::cout << "Usage: " << argv[0] << " [-p <port>] [--help] [-h]"
            << "\n";
  std::cout
      << "\nAllowed arguments:\n    -p <port>  Port for server to listen to\n";
}

// _____________________________________________________________________________
int main(int argc, char** argv) {
  // disable output buffering for standard output
  setbuf(stdout, NULL);

  // initialize randomness
  srand(time(NULL) + rand());  // NOLINT

  int port = 9090;

  for (int i = 1; i < argc; i++) {
    std::string cur = argv[i];
    if (cur == "-h" || cur == "--help") {
      printHelp(argc, argv);
      exit(0);
    } else if (cur == "-p") {
      if (++i >= argc) {
        LOG(ERROR) << "Missing argument for port (-p).";
        exit(1);
      }
      port = atoi(argv[i]);
    }
  }

  LOG(INFO) << "Starting server...";
  Server serv;

  LOG(INFO) << "Listening on port " << port;
  util::http::HttpServer(port, &serv, std::thread::hardware_concurrency())
      .run();
}
