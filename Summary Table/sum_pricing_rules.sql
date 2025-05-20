
Data Source: Ascendeum DS6 DEV
Schema: datasat_9
Table: sum_pricing_rules


-- auto-generated definition
create table sum_pricing_rules
(
    client_id            int          null,
    date                 date         null,
    key_id               varchar(120) null,
    value_id             varchar(120) null,
    ad_unit_id           int          null,
    floor_price_value_id int          null,
    country_id           int          null,
    device_category_id   int          null,
    os_id                int          null,
    custom_dimension_id  int          null,
    revenue              double       null,
    filled_impressions   int          null,
    total_impressions    int          null,
    job_id               int          null,
    constraint uniq_dim
        unique (client_id, date, key_id, value_id, ad_unit_id, country_id, device_category_id, os_id,
                custom_dimension_id, floor_price_value_id, job_id)
)
    partition by range (to_days(`date`));
Show table preview  