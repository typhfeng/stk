#pragma once

#include "codec/L1_DataType.hpp"
#include <cstdint>
#include <vector>

namespace BinaryEncoder {

// ============================================================================
// MAIN ENCODER CLASS
// ============================================================================

class Encoder {
public:
    // Constructor and destructor
    Encoder();
    ~Encoder();

    // Main encoding interface
    std::vector<uint8_t> EncodeMonthSnapshots(const std::vector<L1::BinaryRecord>& records);

private:
    // ========================================================================
    // CORE ENCODING FUNCTIONS
    // ========================================================================

    // Differential encoding
    void ApplyDifferentialEncoding(std::vector<L1::BinaryRecord>& records);
    
    // Compression
    std::vector<uint8_t> CompressData(const std::vector<uint8_t>& raw_data);

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

} // namespace BinaryEncoder
