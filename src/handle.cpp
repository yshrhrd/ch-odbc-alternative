#include "include/handle.h"
#include "include/trace.h"

#include <algorithm>

namespace clickhouse_odbc {

// Safe return value when GetRow fails
ResultRow ResultSet::empty_row_;

size_t ResultSet::RowCount() const {
    if (lazy) return total_row_count;
    return rows.size();
}

const ResultRow &ResultSet::GetRow(size_t idx) {
    if (!lazy) {
        // Normal mode: direct access
        if (idx < rows.size()) return rows[idx];
        return empty_row_;
    }
    // Lazy mode: fetch page and return
    if (idx >= total_row_count) return empty_row_;
    if (!EnsureRow(idx)) return empty_row_;

    std::lock_guard<std::recursive_mutex> pg(page_mutex);
    size_t page_num = idx / page_size;
    size_t offset_in_page = idx % page_size;
    auto it = page_cache.find(page_num);
    if (it != page_cache.end() && offset_in_page < it->second.size()) {
        return it->second[offset_in_page];
    }
    return empty_row_;
}

bool ResultSet::EnsureRow(size_t idx) {
    if (!lazy) return idx < rows.size();
    if (idx >= total_row_count) return false;

    size_t page_num = idx / page_size;

    std::lock_guard<std::recursive_mutex> pg(page_mutex);

    // Check if page exists in cache
    if (page_cache.count(page_num)) {
        TouchPage(page_num);
        return true;
    }

    // Fail if page_fetcher is not set
    if (!page_fetcher) return false;

    // Batch prefetch: fetch multiple pages in a single HTTP request
    // to reduce round-trip overhead during sequential scanning
    size_t max_page = (total_row_count == 0) ? 0 : (total_row_count - 1) / page_size;
    size_t pages_to_fetch = prefetch_pages;
    if (pages_to_fetch < 1) pages_to_fetch = 1;
    // Don't fetch beyond the last page
    if (page_num + pages_to_fetch > max_page + 1) {
        pages_to_fetch = max_page + 1 - page_num;
    }
    // Skip pages already in cache at the start of the batch
    // (only prefetch contiguous missing pages from page_num onward)
    size_t actual_fetch = 0;
    for (size_t i = 0; i < pages_to_fetch; i++) {
        if (!page_cache.count(page_num + i)) {
            actual_fetch = i + 1;  // extend batch to cover this page
        }
    }
    if (actual_fetch < 1) actual_fetch = 1;
    pages_to_fetch = actual_fetch;

    size_t total_limit = pages_to_fetch * page_size;
    size_t offset = page_num * page_size;
    std::string page_query = base_query + " LIMIT " + std::to_string(total_limit)
                           + " OFFSET " + std::to_string(offset);

    TRACE_LOG(TraceLevel::Info, "ResultSet::EnsureRow",
              "Fetching " + std::to_string(pages_to_fetch) + " page(s) starting at page " +
              std::to_string(page_num) +
              " (offset=" + std::to_string(offset) +
              ", limit=" + std::to_string(total_limit) + ")");

    // Evict oldest pages if cache limit exceeded
    EvictOldPages();

    std::vector<ResultRow> batch_rows;
    std::string error;
    if (!page_fetcher(page_query, batch_rows, error)) {
        TRACE_LOG(TraceLevel::Error, "ResultSet::EnsureRow",
                  "Page fetch failed: " + error);
        return false;
    }

    TRACE_LOG(TraceLevel::Debug, "ResultSet::EnsureRow",
              "Fetched " + std::to_string(batch_rows.size()) + " rows for " +
              std::to_string(pages_to_fetch) + " page(s)");

    // Split batch result into page-sized chunks and cache each page
    for (size_t i = 0; i < pages_to_fetch; i++) {
        size_t start = i * page_size;
        if (start >= batch_rows.size()) break;
        size_t end = (std::min)(start + page_size, batch_rows.size());
        // Only cache if not already present (avoid overwriting fresher data)
        if (!page_cache.count(page_num + i)) {
            page_cache[page_num + i] = std::vector<ResultRow>(
                std::make_move_iterator(batch_rows.begin() + start),
                std::make_move_iterator(batch_rows.begin() + end));
            TouchPage(page_num + i);
        }
    }

    return page_cache.count(page_num) > 0;
}

bool ResultSet::Fetch() {
    size_t row_count = RowCount();
    if (current_row + 1 < static_cast<SQLLEN>(row_count)) {
        current_row++;
        if (lazy) {
            // Lazy mode: check if next row is available
            return EnsureRow(static_cast<size_t>(current_row));
        }
        return true;
    }
    return false;
}

void ResultSet::Reset() {
    columns.clear();
    rows.clear();
    current_row = -1;
    // Reset lazy paging
    lazy = false;
    total_row_count = 0;
    base_query.clear();
    page_fetcher = nullptr;
    {
        std::lock_guard<std::recursive_mutex> pg(page_mutex);
        page_cache.clear();
        page_access_order.clear();
    }
}

void ResultSet::CloseCursor() {
    // ODBC spec: SQL_CLOSE preserves IRD (column metadata) but clears rows and cursor
    rows.clear();
    current_row = -1;
    // Clear lazy paging cache (preserve column info)
    if (lazy) {
        std::lock_guard<std::recursive_mutex> pg(page_mutex);
        page_cache.clear();
        page_access_order.clear();
    }
}

void ResultSet::EvictOldPages() {
    while (page_cache.size() >= max_cached_pages && !page_access_order.empty()) {
        // LRU: evict oldest page
        size_t oldest = page_access_order.front();
        page_access_order.erase(page_access_order.begin());
        page_cache.erase(oldest);
        TRACE_LOG(TraceLevel::Debug, "ResultSet::EvictOldPages",
                  "Evicted page " + std::to_string(oldest));
    }
}

void ResultSet::TouchPage(size_t page_num) {
    // LRU update: remove existing entry and append to end
    auto it = std::find(page_access_order.begin(), page_access_order.end(), page_num);
    if (it != page_access_order.end()) {
        page_access_order.erase(it);
    }
    page_access_order.push_back(page_num);
}

} // namespace clickhouse_odbc
