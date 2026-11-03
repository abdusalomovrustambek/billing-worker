-- +goose Up
INSERT INTO billing.fx_rates (currency, rate_ppm) VALUES
    ('USD', 1000000),
    ('EUR', 920000),
    ('GBP', 790000)
ON CONFLICT (currency) DO NOTHING;

-- +goose Down
DELETE FROM billing.fx_rates WHERE currency IN ('USD', 'EUR', 'GBP');
