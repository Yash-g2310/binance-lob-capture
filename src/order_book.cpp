#include "lob_capture/order_book.hpp"

#include "lob_capture/fixed_point.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>

namespace lob_capture {
namespace {

const boost::json::value& required_member(const boost::json::object& object,
                                          const char* name) {
    const auto* value = object.if_contains(name);
    if (value == nullptr) throw std::runtime_error(std::string("missing JSON field '") + name + "'");
    return *value;
}

Level parse_level(const boost::json::value& item) {
    if (!item.is_array()) throw std::runtime_error("order-book level is not an array");
    const auto& pair = item.as_array();
    if (pair.size() < 2 || !pair[0].is_string() || !pair[1].is_string()) {
        throw std::runtime_error("order-book level must contain price and quantity strings");
    }
    const auto price = parse_scaled_decimal(pair[0].as_string());
    const auto quantity = parse_scaled_decimal(pair[1].as_string());
    if (price <= 0) throw std::runtime_error("order-book price must be positive");
    if (quantity < 0) throw std::runtime_error("order-book quantity cannot be negative");
    return Level{price, quantity};
}

std::vector<Level> parse_levels(const boost::json::value& rows, const char* side_name) {
    if (!rows.is_array()) {
        throw std::runtime_error(std::string(side_name) + " updates are not an array");
    }
    std::vector<Level> parsed;
    parsed.reserve(rows.as_array().size());
    for (const auto& item : rows.as_array()) parsed.push_back(parse_level(item));
    return parsed;
}

const boost::json::value& depth_bids(const boost::json::object& object) {
    if (const auto* bids = object.if_contains("b")) return *bids;
    return required_member(object, "bids");
}

const boost::json::value& depth_asks(const boost::json::object& object) {
    if (const auto* asks = object.if_contains("a")) return *asks;
    return required_member(object, "asks");
}

template <typename BidMap, typename AskMap>
TopLevels top_n_from_map(const BidMap& bids, const AskMap& asks) {
    TopLevels result;
    for (auto it = bids.begin(); it != bids.end() && result.bids.size() < 5; ++it) {
        result.bids.push_back(Level{it->first, it->second});
    }
    for (auto it = asks.begin(); it != asks.end() && result.asks.size() < 5; ++it) {
        result.asks.push_back(Level{it->first, it->second});
    }
    return result;
}

}  // namespace

void OrderBook::replace_side(BidMap& levels, const boost::json::value& rows) {
    if (!rows.is_array()) throw std::runtime_error("bid levels are not an array");
    levels.clear();
    for (const auto& item : rows.as_array()) {
        const auto level = parse_level(item);
        if (level.quantity > 0) levels[level.price] = level.quantity;
    }
}

void OrderBook::replace_side(AskMap& levels, const boost::json::value& rows) {
    if (!rows.is_array()) throw std::runtime_error("ask levels are not an array");
    levels.clear();
    for (const auto& item : rows.as_array()) {
        const auto level = parse_level(item);
        if (level.quantity > 0) levels[level.price] = level.quantity;
    }
}

void OrderBook::apply_side(BidMap& levels, const std::vector<Level>& rows) {
    for (const auto& level : rows) {
        if (level.quantity == 0) levels.erase(level.price);
        else levels[level.price] = level.quantity;
    }
}

void OrderBook::apply_side(AskMap& levels, const std::vector<Level>& rows) {
    for (const auto& level : rows) {
        if (level.quantity == 0) levels.erase(level.price);
        else levels[level.price] = level.quantity;
    }
}

void OrderBook::install_snapshot(const boost::json::value& snapshot, Venue /*venue*/) {
    if (!snapshot.is_object()) throw std::runtime_error("REST depth snapshot is not an object");
    const auto& object = snapshot.as_object();
    const auto* update_id = object.if_contains("lastUpdateId");
    if (update_id == nullptr) throw std::runtime_error("REST depth snapshot lacks lastUpdateId");

    BidMap bids;
    AskMap asks;
    replace_side(bids, required_member(object, "bids"));
    replace_side(asks, required_member(object, "asks"));

    bids_ = std::move(bids);
    asks_ = std::move(asks);
    last_update_id_ = json_u64(*update_id, "lastUpdateId");
    has_snapshot_ = true;
    synchronized_ = false;
}

void OrderBook::invalidate() {
    bids_.clear();
    asks_.clear();
    last_update_id_ = 0;
    has_snapshot_ = false;
    synchronized_ = false;
}

ApplyResult OrderBook::apply_diff(const boost::json::value& payload, Venue venue) {
    if (!has_snapshot_) return ApplyResult::NoSnapshot;
    if (!payload.is_object()) throw std::runtime_error("depth payload is not an object");
    const auto& object = payload.as_object();
    const auto* first_id = object.if_contains("U");
    const auto* final_id = object.if_contains("u");
    if (first_id == nullptr || final_id == nullptr) {
        throw std::runtime_error("depth update must contain U and u sequence IDs");
    }
    const std::uint64_t first = json_u64(*first_id, "U");
    const std::uint64_t final = json_u64(*final_id, "u");
    if (first > final) throw std::runtime_error("depth update has U greater than u");

    if (venue == Venue::Spot) {
        if (final <= last_update_id_) return ApplyResult::IgnoredStale;
        if (last_update_id_ == UINT64_MAX || first > last_update_id_ + 1) {
            return ApplyResult::Gap;
        }
    } else {
        if (final < last_update_id_) return ApplyResult::IgnoredStale;
        if (!synchronized_) {
            // The first USD-M event must bridge the snapshot update ID.
            if (first > last_update_id_) return ApplyResult::Gap;
        } else {
            if (final <= last_update_id_) return ApplyResult::IgnoredStale;
            const auto* previous_id = object.if_contains("pu");
            if (previous_id == nullptr || json_u64(*previous_id, "pu") != last_update_id_) {
                return ApplyResult::Gap;
            }
        }
    }

    // Parse and validate every level on both sides before mutating either map.
    // A malformed ask must never leave already-applied bid changes behind.
    const auto parsed_bid_updates = parse_levels(depth_bids(object), "bid");
    const auto parsed_ask_updates = parse_levels(depth_asks(object), "ask");

    apply_side(bids_, parsed_bid_updates);
    apply_side(asks_, parsed_ask_updates);
    last_update_id_ = final;
    synchronized_ = true;
    return ApplyResult::Applied;
}

TopLevels OrderBook::top5() const {
    return top_n_from_map(bids_, asks_);
}

TopLevels top5_from_partial_depth(const boost::json::value& payload) {
    if (!payload.is_object()) throw std::runtime_error("depth5 payload is not an object");
    const auto& object = payload.as_object();
    const auto& bid_rows = depth_bids(object);
    const auto& ask_rows = depth_asks(object);
    if (!bid_rows.is_array() || !ask_rows.is_array()) {
        throw std::runtime_error("depth5 bids/asks are not arrays");
    }

    TopLevels result;
    for (const auto& item : bid_rows.as_array()) {
        const auto level = parse_level(item);
        if (level.quantity > 0) result.bids.push_back(level);
    }
    for (const auto& item : ask_rows.as_array()) {
        const auto level = parse_level(item);
        if (level.quantity > 0) result.asks.push_back(level);
    }
    std::sort(result.bids.begin(), result.bids.end(), [](const Level& left, const Level& right) {
        return left.price > right.price;
    });
    std::sort(result.asks.begin(), result.asks.end(), [](const Level& left, const Level& right) {
        return left.price < right.price;
    });
    if (result.bids.size() > 5) result.bids.resize(5);
    if (result.asks.size() > 5) result.asks.resize(5);
    return result;
}

char side_for_depth_event(const boost::json::value& payload) {
    if (!payload.is_object()) return 'N';
    const auto& object = payload.as_object();
    const auto* bids = object.if_contains("b");
    const auto* asks = object.if_contains("a");
    const bool has_bids = bids != nullptr && bids->is_array() && !bids->as_array().empty();
    const bool has_asks = asks != nullptr && asks->is_array() && !asks->as_array().empty();
    if (has_bids && !has_asks) return 'B';
    if (has_asks && !has_bids) return 'S';
    return 'N';
}

}  // namespace lob_capture
