create
    definer = datasat_9@`%` procedure sp_fact2sum_price_floors_results(IN vdate date)
BEGIN

INSERT INTO `datasat_9`.`sum_price_floors_results`
    (`client_id`, `date`, `site_id`, `ad_unit_combination_id`,
     `revenue`, `impressions`, `unfilled_impressions`, `ft4_impressions`,
     `median_floor_value`, `cpm`, `fillrate`, `rev_uplift`, `rev_uplift_pct`)
SELECT
    `client_id`, `date`, `site_id`, `ad_unit_combination_id`,
    `revenue`, `impressions`, `unfilled_impressions`, `ft4_impressions`,
    `median_floor_value`, `cpm`, `fillrate`, `rev_uplift`, `rev_uplift_pct`
FROM `datasat_9`.`fact_price_floors_results`
WHERE `date` = vdate
ON DUPLICATE KEY UPDATE
    `revenue` = VALUES(`revenue`),
    `impressions` = VALUES(`impressions`),
    `unfilled_impressions` = VALUES(`unfilled_impressions`),
    `ft4_impressions` = VALUES(`ft4_impressions`),
    `median_floor_value` = VALUES(`median_floor_value`),
    `cpm` = VALUES(`cpm`),
    `fillrate` = VALUES(`fillrate`),
    `rev_uplift` = VALUES(`rev_uplift`),
    `rev_uplift_pct` = VALUES(`rev_uplift_pct`);

END; 