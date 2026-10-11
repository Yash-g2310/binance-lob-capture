#pragma once

#include "lob_capture/types.hpp"

#include <filesystem>

namespace lob_capture {

// Rebuilds the order-book CSV from one market-data CSV and its adjacent
// *_snapshots.jsonl sidecar. No network calls are made in replay mode.
void replay_market_csv(const std::filesystem::path& market_csv,
                       const std::filesystem::path& output_dir,
                       Metrics& metrics);

}  // namespace lob_capture
