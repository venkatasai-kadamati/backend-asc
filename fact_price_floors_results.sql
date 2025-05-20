
Data Source: Ascendeum DS6 DEV
Schema: datasat_9
Table: fact_price_floors_results


-- auto-generated definition
create table fact_price_floors_results
(
    id                     int auto_increment,
    date                   date                                not null,
    client_id              int                                 not null,
    site_id                int                                 null,
    ad_unit_combination_id int                                 not null,
    revenue                double                              null,
    impressions            int                                 null,
    unfilled_impressions   int                                 null,
    ft4_impressions        int                                 null,
    median_floor_value     double                              null,
    cpm                    double                              null,
    fillrate               double                              null,
    rev_uplift             double                              null,
    rev_uplift_pct         double                              null,
    active_floor_id        int                                 null,
    updated_at             timestamp default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    primary key (id, date, client_id, ad_unit_combination_id),
    constraint unique_entry
        unique (id, date, client_id, site_id, ad_unit_combination_id)
)
    partition by range (to_days(`date`));

grant insert, select, update on table fact_price_floors_results to autowriter_9;

grant delete on table fact_price_floors_results to bharathtoenterprise;

grant delete, insert, select, update on table fact_price_floors_results to kavyatoenterprise;
