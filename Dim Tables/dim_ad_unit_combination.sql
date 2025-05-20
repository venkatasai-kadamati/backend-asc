
Data Source: Ascendeum DS6 DEV
Schema: datasat_9
Table: dim_ad_unit_combination


-- auto-generated definition
create table dim_ad_unit_combination
(
    id                        int auto_increment
        primary key,
    client_id                 int          not null,
    ad_unit                   varchar(255) null,
    device_category           varchar(255) null,
    os                        varchar(255) null,
    geo                       varchar(255) null,
    site_id                   int          null,
    custom_key_value_other_id int          null
);

grant insert, select, update on table dim_ad_unit_combination to autowriter_9;

grant delete, insert, update on table dim_ad_unit_combination to bharathtoenterprise;

grant insert, select, update on table dim_ad_unit_combination to kavyatoenterprise;
