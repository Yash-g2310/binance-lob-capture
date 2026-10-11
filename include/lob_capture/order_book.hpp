#pragma once

#include "lob_capture/types.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace lob_capture {

class OrderBook {
public:
    // Installs a REST depth snapshot. The book is provisional until a sequence-
    // compatible differential update is applied.
    void install_snapshot(const boost::json::value& snapshot, Venue venue);
    void invalidate();

    ApplyResult apply_diff(const boost::json::value& payload, Venue venue);
    bool has_snapshot() const noexcept { return has_snapshot_; }
    bool synchronized() const noexcept { return synchronized_; }
    std::uint64_t last_update_id() const noexcept { return last_update_id_; }
    TopLevels top5() const;

private:
    using BidMap = std::map<std::int64_t, std::int64_t, std::greater<std::int64_t>>;
    using AskMap = std::map<std::int64_t, std::int64_t>;

    static void replace_side(BidMap& levels, const boost::json::value& rows);
    static void replace_side(AskMap& levels, const boost::json::value& rows);
    static void apply_side(BidMap& levels, const std::vector<Level>& rows);
    static void apply_side(AskMap& levels, const std::vector<Level>& rows);

    BidMap bids_;
    AskMap asks_;
    std::uint64_t last_update_id_{0};
    bool has_snapshot_{false};
    bool synchronized_{false};
};

TopLevels top5_from_partial_depth(const boost::json::value& payload);
char side_for_depth_event(const boost::json::value& payload);

}  // namespace lob_capture
