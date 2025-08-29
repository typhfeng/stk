#pragma once

#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace misc {

inline void print_progress(size_t current, size_t total, const std::string &message = "") {
  const int bar_width = 40;
  float progress = static_cast<float>(current) / total;
  int pos = static_cast<int>(bar_width * progress);

  // Build complete output string to avoid threading issues
  std::ostringstream output;
  output << "\r[";
  for (int i = 0; i < bar_width; ++i) {
    if (i < pos)
      output << "=";
    else if (i == pos)
      output << ">";
    else
      output << " ";
  }
  output << "] " << std::setw(3) << static_cast<size_t>(progress * 100.0f)
         << "% (" << current << "/" << total << ")";
  if (!message.empty()) {
    output << " " << message;
  }
  
  std::cout << output.str() << std::flush;
  if (current == total) {
    std::cout << "\n";
  }
}

class Timer {
public:
  Timer(const std::string &label = "")
      : label_(label), start_(std::chrono::high_resolution_clock::now()) {
    std::cout << "\n";
  }

  ~Timer() {
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float> elapsed = end - start_;
    std::cout << label_ << " " << elapsed.count() * 1000 << "ms";
  }

private:
  std::string label_;
  std::chrono::time_point<std::chrono::high_resolution_clock> start_;
};

} // namespace misc