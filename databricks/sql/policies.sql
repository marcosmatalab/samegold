-- Row filter and column mask, declared as SQL and applied by the bundle.
--
-- Free Edition has no account groups (no SCIM, no SSO), so `is_account_group_member` resolves
-- to false for everyone and these policies are demonstrated rather than enforced. That is
-- stated in docs/limits.md instead of being presented as a working control.

CREATE OR REPLACE FUNCTION ${catalog}.main.only_own_country(country STRING)
RETURN is_account_group_member('finance_all') OR country = current_user_country();

CREATE OR REPLACE FUNCTION ${catalog}.main.mask_customer(customer_id STRING)
RETURN CASE WHEN is_account_group_member('finance_all') THEN customer_id
            ELSE sha2(customer_id, 256) END;

ALTER TABLE ${catalog}.main.dim_customer_scd2
  SET ROW FILTER ${catalog}.main.only_own_country ON (country);

ALTER TABLE ${catalog}.main.dim_customer_scd2
  ALTER COLUMN customer_id SET MASK ${catalog}.main.mask_customer;
