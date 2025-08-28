#pragma once

#include "codec/L1_DataType.hpp"
#include <cstdint>
#include <vector>

namespace BinaryEncoder_L1 {

// ============================================================================
// MAIN ENCODER CLASS
// ============================================================================

class Encoder {
public:
    // Constructor and destructor
    Encoder();
    ~Encoder();

    // Main encoding interface
    std::vector<uint8_t> EncodeMonthSnapshots(const std::vector<L1::Snapshot>& records);

private:
    // ========================================================================
    // CORE ENCODING FUNCTIONS
    // ========================================================================

    // Differential encoding
    void ApplyDifferentialEncoding(std::vector<L1::Snapshot>& records);

    // ========================================================================
    // UTILITY FUNCTIONS
    // ========================================================================

    // Use L1::PriceToTick from L1_DataType.hpp

    // ========================================================================
    // MEMBER VARIABLES
    // ========================================================================

    // Buffer configuration
    static constexpr size_t BUFFER_SIZE = 1024 * 1024; // 1MB buffer

    // I/O buffers
    std::vector<uint8_t> buffer_;
};

} // namespace BinaryEncoder_L1
