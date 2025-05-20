
-- stored proc : sp_stg2fact_price_floors_results
DELIMITER $$

CREATE
    DEFINER = `datasat_9`@`%`
    PROCEDURE `sp_stg2fact_price_floors_results`(
    IN vbatch_id INT
)
BEGIN
    DECLARE vclient_id INT;
    DECLARE v_now      DATETIME;

    -- capture one timestamp for all DMLs
    SET v_now = NOW();

    -- validate batch
    CALL sp_stg_validate_batch_id(vbatch_id, 'pricefloorsresults', 'default');

    -- mark batch in‐progress
    UPDATE datasat_9.stg_batch
    SET   status     = 2,
          updated_on = v_now
    WHERE id = vbatch_id;

    -- lookup client, then root parent client
    SELECT client_id
    INTO   vclient_id
    FROM   datasat_9.stg_batch
    WHERE  id = vbatch_id;

    SELECT IFNULL(parent_client_id, id)
    INTO   vclient_id
    FROM   datasat_9.dim_client
    WHERE  id = vclient_id;

    -- purge bad rows
    DELETE
    FROM   stage.stg_price_floors_results
    WHERE  date     IS NULL
      AND  batch_id = vbatch_id;

    -- seed dim_site so we can look up site_id by (url, client)
    INSERT INTO datasat_9.dim_site (client_id, url, created_by, created_at)
    SELECT DISTINCT vclient_id, dat.property, 0, v_now
    FROM   stage.stg_price_floors_results AS dat
    WHERE  dat.batch_id = vbatch_id
      AND  dat.property IS NOT NULL
      AND  dat.property <> ''
      AND NOT EXISTS (
        SELECT 1
        FROM   datasat_9.dim_site AS ds
        WHERE  ds.client_id = vclient_id
          AND  ds.url       = dat.property
    );

    -- standard dimension seeding : geo, device category, os
    INSERT INTO datasat_9.dim_geo (name, created_by, created_at)
    SELECT DISTINCT dat.geo, 0, v_now
    FROM   stage.stg_price_floors_results AS dat
    WHERE  dat.batch_id = vbatch_id
      AND  dat.geo IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM   datasat_9.dim_geo AS dim
        WHERE  dim.name = dat.geo
    );

    INSERT INTO datasat_9.dim_device_category (name, created_by, created_at)
    SELECT DISTINCT dat.device_category, 0, v_now
    FROM   stage.stg_price_floors_results AS dat
    WHERE  dat.batch_id = vbatch_id
      AND  dat.device_category IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM   datasat_9.dim_device_category AS dim
        WHERE  dim.name = dat.device_category
    );

    INSERT INTO datasat_9.dim_os (name, created_at, created_by)
    SELECT DISTINCT dat.os, v_now, 0
    FROM   stage.stg_price_floors_results AS dat
    WHERE  dat.batch_id = vbatch_id
      AND  dat.os IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM   datasat_9.dim_os AS dim
        WHERE  dim.name = dat.os
    );

    INSERT INTO datasat_9.dim_custom_key_value_other (custom_key_value_other)
    SELECT DISTINCT dat.custom_key_value_other
    FROM   stage.stg_price_floors_results AS dat
    WHERE  dat.batch_id = vbatch_id
      AND  dat.custom_key_value_other IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM   datasat_9.dim_custom_key_value_other AS dim
        WHERE  dim.custom_key_value_other = dat.custom_key_value_other
    );

    -- build ad_unit_combinations using dim_site.id
    INSERT INTO datasat_9.dim_ad_unit_combination
    ( client_id, ad_unit, device_category, os, geo, site_id, custom_key_value_other_id )
    SELECT DISTINCT
           vclient_id,
           dat.ad_unit,
           dat.device_category,
           dat.os,
           dat.geo,
           ds.id                     AS site_id,
           cko.id                    AS custom_key_value_other_id
    FROM   stage.stg_price_floors_results AS dat
    LEFT   JOIN datasat_9.dim_site AS ds
      ON   ds.client_id = vclient_id
      AND  ds.url       = dat.property
    LEFT   JOIN datasat_9.dim_custom_key_value_other AS cko
      ON   cko.custom_key_value_other = dat.custom_key_value_other
    WHERE  dat.batch_id = vbatch_id
      AND  dat.ad_unit   IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM   datasat_9.dim_ad_unit_combination AS dim
        WHERE  dim.client_id                = vclient_id
          AND  dim.ad_unit                  = dat.ad_unit
          AND  dim.device_category          = dat.device_category
          AND  dim.os                       = dat.os
          AND  dim.geo                      = dat.geo
          AND  dim.site_id                  = ds.id
          AND (
               (dim.custom_key_value_other_id IS NULL
                AND dat.custom_key_value_other IS NULL)
            OR (dim.custom_key_value_other_id = cko.id)
          )
    );

    -- insert into the fact table, wiring in dim_site.id
    INSERT INTO datasat_9.fact_price_floors_results
    ( date, client_id, site_id, ad_unit_combination_id,
      revenue, impressions, unfilled_impressions, ft4_impressions,
      median_floor_value, cpm, fillrate, rev_uplift, rev_uplift_pct )
    SELECT
        dat.date,
        vclient_id                  AS client_id,
        ds.id                       AS site_id,
        IFNULL(dac.id, 0)           AS ad_unit_combination_id,
        dat.revenue,
        dat.impressions,
        dat.unfilled_impressions,
        dat.ft4_impressions,
        dat.median_floor_value,
        CASE WHEN dat.impressions > 0
             THEN (dat.revenue / dat.impressions) * 1000
             ELSE 0
        END                         AS cpm,
        CASE WHEN (dat.impressions + dat.unfilled_impressions) > 0
             THEN dat.impressions * 100
                  / (dat.impressions + dat.unfilled_impressions)
             ELSE 0
        END                         AS fillrate,
        dat.rev_uplift,
        CASE WHEN dat.revenue > 0
             THEN dat.rev_uplift * 100 / dat.revenue
             ELSE 0
        END                         AS rev_uplift_pct
    FROM   stage.stg_price_floors_results AS dat
    LEFT   JOIN datasat_9.dim_site AS ds
      ON   ds.client_id = vclient_id
      AND  ds.url       = dat.property
    LEFT   JOIN datasat_9.dim_custom_key_value_other AS cko
      ON   cko.custom_key_value_other = dat.custom_key_value_other
    LEFT   JOIN datasat_9.dim_ad_unit_combination AS dac
      ON   dac.client_id                   = vclient_id
       AND dac.ad_unit                     = dat.ad_unit
       AND dac.device_category             = dat.device_category
       AND dac.os                          = dat.os
       AND dac.geo                         = dat.geo
       AND dac.site_id                     = ds.id
       AND dac.custom_key_value_other_id   = cko.id
    WHERE  dat.batch_id = vbatch_id
    ON DUPLICATE KEY UPDATE
        revenue              = VALUES(revenue),
        impressions          = VALUES(impressions),
        unfilled_impressions = VALUES(unfilled_impressions),
        ft4_impressions      = VALUES(ft4_impressions),
        median_floor_value   = VALUES(median_floor_value),
        cpm                  = VALUES(cpm),
        fillrate             = VALUES(fillrate),
        rev_uplift           = VALUES(rev_uplift),
        rev_uplift_pct       = VALUES(rev_uplift_pct),
        updated_at           = v_now;

    -- mark batch complete
    UPDATE datasat_9.stg_batch
    SET   status     = 99,
          updated_on = v_now
    WHERE id = vbatch_id;

    -- next sproc:  summarization
    -- CALL sp_fact2sum_price_floors_results(vbatch_id);
END$$

DELIMITER ;