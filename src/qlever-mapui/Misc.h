#ifndef MAPUI_MISC_H_
#define MAPUI_MISC_H_

const static size_t I_OFFSET = 4611686018427387904;

enum ParseState {
  IN_HEADER,
  IN_ROW
};

union ID {
  uint64_t val;
  uint8_t bytes[8];
};

#endif  // MAPUI_MISC_H_
