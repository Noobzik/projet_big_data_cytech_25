-- Insert Reference Data

-- Vendor
INSERT INTO dim_vendor (vendor_id, vendor_name) VALUES
(1, 'Creative Mobile Technologies, LLC'),
(2, 'VeriFone Inc.')
ON CONFLICT (vendor_id) DO NOTHING;

-- Rate Code
INSERT INTO dim_rate_code (rate_code_id, rate_code_name) VALUES
(1, 'Standard rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated fare'),
(6, 'Group ride'),
(99, 'Unknown')
ON CONFLICT (rate_code_id) DO NOTHING;

-- Payment Type
INSERT INTO dim_payment_type (payment_type_id, payment_type_name) VALUES
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip')
ON CONFLICT (payment_type_id) DO NOTHING;

-- Location (Placeholder - In a real scenario, we would load the full CSV)
-- Inserting a generic 'Unknown' location to avoid FK violations if lookup fails initially
INSERT INTO dim_location (location_id, borough, zone, service_zone) VALUES
(1, 'EWR', 'Newark Airport', 'EWR'),
(2, 'Queens', 'Jamaica Bay', 'Boro Zone'),
(264, 'Unknown', 'NV', 'N/A'),
(265, 'Unknown', 'NA', 'N/A')
ON CONFLICT (location_id) DO NOTHING;
