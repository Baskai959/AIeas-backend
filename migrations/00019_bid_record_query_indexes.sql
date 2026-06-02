-- +goose Up
ALTER TABLE `bid_record`
  ADD KEY `idx_bid_record_auction_allow_price_time` (`auction_id`, `risk_result`, `reject_reason`, `bid_price` DESC, `bid_ts_ms` ASC),
  ADD KEY `idx_bid_record_auction_allow_time` (`auction_id`, `risk_result`, `reject_reason`, `bid_ts_ms`);

-- +goose Down
ALTER TABLE `bid_record`
  DROP KEY `idx_bid_record_auction_allow_time`,
  DROP KEY `idx_bid_record_auction_allow_price_time`;
