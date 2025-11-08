#pragma once

#include "misc/progress_parallel.hpp"

// Forward declarations
struct SharedState;
class GlobalFeatureStore;

// ============================================================================
// CROSS-SECTIONAL WORKER (DATE-FIRST, SINGLE-THREADED)
// ============================================================================

void crosssectional_worker(const SharedState& state,
                           GlobalFeatureStore* feature_store,
                           int worker_id,
                           misc::ProgressHandle progress_handle);

