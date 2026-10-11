#pragma once

#include "lob_capture/types.hpp"

#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace lob_capture {

std::string csv_escape(std::string_view field);
std::vector<std::string> parse_csv_record(std::string_view line);
const std::string& market_csv_header();
const std::string& orderbook_csv_header();
std::string format_orderbook_row(std::int64_t timestamp_ns, std::uint64_t seq_no,
                                 std::int32_t instrument_id, char type, char side,
                                 const TopLevels& levels);

class CsvOutputManager {
public:
    CsvOutputManager(std::filesystem::path output_dir, Venue venue,
                     const std::vector<std::string>& symbols);
    ~CsvOutputManager();

    CsvOutputManager(const CsvOutputManager&) = delete;
    CsvOutputManager& operator=(const CsvOutputManager&) = delete;

    void write_market(const MarketEvent& event);
    void write_orderbook(const MarketEvent& event, char type, char side,
                         const TopLevels& levels);
    void write_snapshot_record(const MarketEvent& event,
                               const boost::json::value& snapshot);
    void flush();

    std::filesystem::path market_path(const std::string& symbol) const;
    std::filesystem::path orderbook_path(const std::string& symbol) const;
    std::filesystem::path snapshot_path(const std::string& symbol) const;
    std::int32_t instrument_id(const std::string& symbol) const;

private:
    struct SymbolFiles {
        std::ofstream market;
        std::ofstream orderbook;
        std::ofstream snapshots;
        std::filesystem::path market_path;
        std::filesystem::path orderbook_path;
        std::filesystem::path snapshot_path;
        std::uint64_t next_orderbook_seq{1};
    };

    SymbolFiles& files_for(const std::string& symbol);
    std::filesystem::path output_dir_;
    Venue venue_;
    std::string date_;
    std::map<std::string, std::int32_t> instrument_ids_;
    std::map<std::string, std::unique_ptr<SymbolFiles>> files_;
};

}  // namespace lob_capture
