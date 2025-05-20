-- Data Source: Ascendeum DS6 DEV
-- Schema: datasat_9
-- Table: sum_price_floors_results

create table sum_price_floors_results
(
    client_id              int          null,
    date                   date         null,
    site_id                int          null,
    ad_unit_combination_id int          null,
    revenue                double       null,
    impressions            int          null,
    unfilled_impressions   int          null,
    ft4_impressions        int          null,
    median_floor_value     double       null,
    cpm                    double       null,
    fillrate               double       null,
    rev_uplift             double       null,
    rev_uplift_pct         double       null,
    constraint uniq_pf_results
        unique (client_id, date, site_id, ad_unit_combination_id)
)
    partition by range (to_days(`date`)); 