create
    definer = datasat_9@`%` procedure sp_fact2sum_pricing_rules(IN vbatch_id int)
BEGIN

INSERT INTO `datasat_9`.`sum_pricing_rules`
(`client_id`,
`date`,
`key_id`,
`value_id`,
`ad_unit_id`,
`floor_price_value_id`,
`country_id`,
`device_category_id`,
`os_id`,
`custom_dimension_id`,
`revenue`,
`filled_impressions`,
`total_impressions`,
`job_id`)
SELECT
`client_id`, `date`, `key_id`, `value_id`, `ad_unit_id`, `floor_price_value_id`,
`country_id`, `device_category_id`, `os_id`, `custom_dimension_id`, `revenue`, 
`filled_impressions`, `total_impressions`, `job_id`
FROM `datasat_9`.`fact_pricing_rules`
WHERE batch_id = vbatch_id
ON DUPLICATE KEY UPDATE
  `revenue` = VALUES(`revenue`),
  `filled_impressions` = VALUES(`filled_impressions`),
  `total_impressions` = VALUES(`total_impressions`);

END;

