
Data Source: Ascendeum DS6 DEV
Schema: datasat_9
Routine: sp_stg2fact_pricing_rules


create definer = datasat_9@`%` procedure sp_stg2fact_pricing_rules(IN vbatch_id int)
BEGIN

DECLARE vclient_id INT;

call sp_stg_validate_batch_id(vbatch_id,'pricingrules','default');
UPDATE datasat_9.stg_batch SET status = 2, updated_on = now() WHERE id = vbatch_id;
select `client_id` into vclient_id from `datasat_9`.`stg_batch` where `id`=vbatch_id;
select ifnull(parent_client_id,id) into vclient_id from dim_client where id=vclient_id;
delete from stage.stg_pricing_rules where date is null and batch_id=vbatch_id;
select id into @job_type_id from ultron.job_type where type='upload_pricing_rules_data';

INSERT INTO datasat_9.dim_keyvalue_key(client_id, name, display_name, created_by, created_at)
SELECT DISTINCT vclient_id, dat.key, dat.key, 0, now()
FROM stage.stg_pricing_rules dat
WHERE batch_id = vbatch_id AND dat.key IS NOT NULL
AND
NOT EXISTS (SELECT 1 FROM datasat_9.dim_keyvalue_key dim WHERE dim.name = dat.key and dim.client_id = vclient_id);

INSERT INTO datasat_9.dim_keyvalue_value(value, created_by, created_at)
SELECT DISTINCT dat.value, @job_type_id ,now()
FROM stage.stg_pricing_rules dat
WHERE batch_id = vbatch_id AND dat.value IS NOT NULL
AND
NOT EXISTS (SELECT 1 FROM datasat_9.dim_keyvalue_value dim WHERE dim.value = dat.value);

INSERT INTO datasat_9.dim_geo(name, created_by, created_at)
SELECT DISTINCT dat.country,0,now()
FROM stage.stg_pricing_rules dat
WHERE batch_id = vbatch_id AND dat.country IS NOT NULL
AND
NOT EXISTS (SELECT 1 FROM datasat_9.dim_geo dim WHERE dim.name = dat.country);


INSERT INTO datasat_9.dim_device_category(name, created_at, created_by)
SELECT DISTINCT dat.device_category, 0, now()
FROM stage.stg_pricing_rules dat
WHERE batch_id = vbatch_id AND dat.device_category IS NOT NULL
AND
NOT EXISTS (SELECT 1 FROM datasat_9.dim_device_category dim WHERE dim.name = dat.device_category);

INSERT INTO datasat_9.dim_os(name, created_at, created_by)
SELECT DISTINCT dat.os, now(), 0
FROM stage.stg_pricing_rules dat
WHERE batch_id = vbatch_id AND dat.os IS NOT NULL
AND
NOT EXISTS (SELECT 1 FROM datasat_9.dim_os dim WHERE dim.name = dat.os);

INSERT INTO datasat_9.dim_price_floor_value(price_floor_value)
SELECT DISTINCT dat.floor_price_value
FROM stage.stg_pricing_rules dat
WHERE batch_id = vbatch_id AND dat.floor_price_value IS NOT NULL
AND
NOT EXISTS (SELECT 1 FROM datasat_9.dim_price_floor_value dim WHERE dim.price_floor_value = dat.floor_price_value);

INSERT INTO datasat_9.dim_custom_dimension(`dimension`)
SELECT DISTINCT dat.custom_dimensions
FROM stage.stg_pricing_rules dat
WHERE batch_id = vbatch_id AND dat.custom_dimensions IS NOT NULL
AND
NOT EXISTS (SELECT 1 from datasat_9.dim_custom_dimension dim WHERE dim.`dimension`= dat.custom_dimensions);

INSERT INTO `datasat_9`.`fact_pricing_rules`
    (`client_id`,
    `batch_id`,
    `date`,
    `key_id`,
    `value_id`,
    `ad_unit_id`,
    `country_id`,
    `device_category_id`,
    `os_id`,
    `custom_dimension_id`,
    `revenue`,
    `filled_impressions`,
    `total_impressions`,
    `floor_price_value_id`,
    `job_id`,
    `updated_at`)
SELECT
      vclient_id client_id,
      vbatch_id batch_id,
      dat.date date,
      dk.id key_id,
      dv.id value_id,
      if(au.`id` IS NULL, 0, au.`id`) ad_unit_id,
      if(dg.`id` IS NULL, 0, dg.`id`) country_id,
      if(dc.`id` IS NULL, 0, dc.`id`) device_category_id,
      if(os.`id` IS NULL, 0, os.`id`) os_id,
      if(cd.`id` IS NULL, 0, cd.`id`) custom_dimension_id,
      dat.revenue,
      dat.filled_impressions,
      dat.total_impressions,
      pf.id floor_price_value_id,
      if (dat.job_id IS NULL, 0, dat.job_id) job_id,
      now() updated_at
  FROM stage.stg_pricing_rules dat
  LEFT JOIN datasat_9.dim_keyvalue_key dk ON dat.key=dk.name AND dk.client_id=vclient_id
  LEFT JOIN datasat_9.dim_keyvalue_value dv ON dat.value=dv.value
  LEFT JOIN (select min(id) id, name from dim_ad_unit group by name ) au on get_valid_ad_unit_name(dat.ad_unit)=au.name
  LEFT JOIN datasat_9.dim_geo dg ON dat.country=dg.name
  LEFT JOIN datasat_9.dim_device_category dc ON dat.device_category=dc.name
  LEFT JOIN datasat_9.dim_os os ON dat.os=os.name
  LEFT JOIN datasat_9.dim_custom_dimension cd ON dat.custom_dimensions = cd.dimension
  LEFT JOIN datasat_9.dim_price_floor_value pf ON pf.price_floor_value = dat.floor_price_value
  WHERE dat.batch_id = vbatch_id
  ON DUPLICATE KEY UPDATE
  `revenue` = VALUES(`revenue`),
  `filled_impressions` = VALUES(`filled_impressions`),
  `total_impressions` = VALUES(`total_impressions`),
  `updated_at` = now(),
  `batch_id` = vbatch_id,
  `version` = `version`+1;

UPDATE datasat_9.stg_batch SET status = 99, updated_on = now() WHERE id = vbatch_id;

CALL sp_fact2sum_pricing_rules(vbatch_id);

END;


-- separator


Data Source: Ascendeum DS6 DEV
Schema: datasat_9
Routine: sp_stg2fact_webanalytics


create
    definer = datasat_9@`%` procedure sp_stg2fact_webanalytics(IN vbatch_id int)
BEGIN
    DECLARE vclient_id, valertflag INT;
    DECLARE vmin_batch_date DATE;

    
    CALL sp_stg_validate_batch_id(vbatch_id, 'webanalytics', 'default');
    
    
    UPDATE stg_batch SET status = 2, updated_on = NOW() WHERE id = vbatch_id;
    
    
    SELECT `client_id` INTO vclient_id FROM `stg_batch` WHERE `id` = vbatch_id;
    SELECT IFNULL(parent_client_id, id) INTO vclient_id FROM dim_client WHERE id = vclient_id;

    
    IF vclient_id IS NULL THEN
        SIGNAL SQLSTATE '45009' SET MESSAGE_TEXT = 'Client ID cannot be NULL.';
    END IF;

    
    SELECT MIN(date) INTO vmin_batch_date FROM stg_webanalytics WHERE batch_id = vbatch_id;
    IF (vclient_id <= 71) AND (vmin_batch_date < '2021-06-01') THEN
        SIGNAL SQLSTATE '45010' SET MESSAGE_TEXT = 'Cannot upload data before 1st June 2021 for client_id <= 71';
    END IF;

    
    DELETE FROM stg_webanalytics WHERE (date IS NULL OR date < '1900-01-01') AND batch_id = vbatch_id;

    
    
    INSERT INTO dim_site (client_id, url, created_by, created_at)
    SELECT DISTINCT vclient_id, property_name, 0, NOW()
    FROM stg_webanalytics dat
    INNER JOIN stg_batch bat ON bat.id = dat.batch_id
    WHERE dat.batch_id = vbatch_id
    AND NOT EXISTS (
        SELECT 1
        FROM dim_site sit
        WHERE sit.index_name = LOWER(REPLACE(REPLACE(dat.property_name, ' ', ''), ' ', ''))
    )
    AND dat.property_name IS NOT NULL
    AND dat.property_name <> '';

    
    INSERT INTO dim_webanalytics_view (name, created_by, created_at)
    SELECT DISTINCT dat.view_name, 0, NOW()
    FROM stg_webanalytics dat
    WHERE dat.batch_id = vbatch_id
    AND dat.view_name IS NOT NULL
    AND dat.view_name <> ''
    AND dat.view_name <> '(not set)'
    AND NOT EXISTS (
        SELECT 1
        FROM dim_webanalytics_view dim
        WHERE dim.name = dat.view_name
    );

    
    INSERT INTO dim_device_category (name, created_by, created_at)
    SELECT DISTINCT dat.device_category, 0, NOW()
    FROM stg_webanalytics dat
    WHERE dat.batch_id = vbatch_id
    AND dat.device_category IS NOT NULL
    AND dat.device_category <> ''
    AND dat.device_category <> '(not set)'
    AND NOT EXISTS (
        SELECT 1
        FROM dim_device_category dim
        WHERE dim.index_name = LOWER(REPLACE(dat.device_category, ' ', ''))
    );

    
    INSERT INTO dim_source (name, created_by, created_at)
    SELECT DISTINCT dat.source, 0, NOW()
    FROM stg_webanalytics dat
    WHERE dat.batch_id = vbatch_id
    AND dat.source IS NOT NULL
    AND dat.source <> ''
    AND dat.source <> '(not set)'
    AND NOT EXISTS (
        SELECT 1
        FROM dim_source dim
        WHERE dim.index_name = LOWER(REPLACE(dat.source, ' ', ''))
    );

    
    INSERT INTO dim_os (name, created_by, created_at)
    SELECT DISTINCT dat.os, 0, NOW()
    FROM stg_webanalytics dat
    WHERE dat.batch_id = vbatch_id
    AND dat.os IS NOT NULL
    AND dat.os <> ''
    AND dat.os <> '(not set)'
    AND NOT EXISTS (
        SELECT 1
        FROM dim_os dim
        WHERE dim.name = dat.os
    );

    
    INSERT INTO dim_geo (name, created_by, created_at)
    SELECT DISTINCT dat.country, 0, NOW()
    FROM stg_webanalytics dat
    WHERE dat.batch_id = vbatch_id
    AND dat.country IS NOT NULL
    AND dat.country <> ''
    AND dat.country <> '(not set)'
    AND NOT EXISTS (
        SELECT 1
        FROM dim_geo dim
        WHERE dim.name = dat.country
    );

    
    INSERT INTO `fact_webanalytics` (`date`, `client_id`, `site_id`, `view_id`, `geo_id`, `device_category_id`, `os_id`, `source_id`, `users`, `sessions`, `page_views`, `bounces`, `session_duration`, `updated_at`, `batch_id`)
    SELECT dat.`date`, vclient_id, 
           IFNULL(sit.`id`, 0), IFNULL(vw.`id`, 0), 
           IFNULL(geo.`id`, 0), IFNULL(dc.`id`, 0), 
           IFNULL(os.`id`, 0), IFNULL(source1.`id`, 0), 
           dat.`users`, dat.`sessions`, dat.`page_views`, dat.`bounces`, 
           dat.`session_duration`, NOW(), vbatch_id
    FROM stg_webanalytics dat
    LEFT JOIN dim_device_category dc ON LOWER(REPLACE(dat.device_category, ' ', '')) = dc.index_name
    LEFT JOIN dim_geo geo ON dat.country = geo.name
    LEFT JOIN dim_os os ON dat.os = os.name
    LEFT JOIN dim_source source1 ON LOWER(REPLACE(dat.source, ' ', '')) = source1.index_name
    LEFT JOIN dim_webanalytics_view vw ON dat.view_name = vw.name
    LEFT JOIN stg_batch sb ON sb.id = dat.batch_id
    LEFT JOIN dim_site sit ON LOWER(REPLACE(REPLACE(dat.property_name, ' ', ''), ' ', '')) = sit.index_name 
                           AND sit.client_id = sb.client_id
    WHERE dat.batch_id = vbatch_id
    ON DUPLICATE KEY UPDATE `users` = VALUES(`users`), `sessions` = VALUES(`sessions`), 
                            `page_views` = VALUES(`page_views`), `bounces` = VALUES(`bounces`), 
                            `session_duration` = VALUES(`session_duration`), 
                            `updated_at` = NOW(), `batch_id` = vbatch_id, `version` = `version` + 1;

    
    UPDATE stg_batch SET status = 90, updated_on = NOW() WHERE id = vbatch_id;

    
    CALL sp_refresh_summary_batch_id(vbatch_id);

    
    INSERT IGNORE INTO `log_fact_table_update` (`table_name`, `date`, `client_id`, `batch_id`)
    SELECT DISTINCT 'fact_webanalytics', date, vclient_id, vbatch_id
    FROM stg_webanalytics
    WHERE batch_id = vbatch_id;

    
    UPDATE stg_batch SET status = 99, updated_on = NOW() WHERE id = vbatch_id;

END;

grant execute on procedure sp_stg2fact_webanalytics to autowriter_9;

grant execute on procedure sp_stg2fact_webanalytics to sampathtoenterprise;

-- separator


Data Source: Ascendeum DS6 DEV
Schema: datasat_9
Routine: sp_stg2fact_adserver_date


create
    definer = datasat_9@`%` procedure sp_stg2fact_adserver_date(IN vbatch_id int)
X:BEGIN
   DECLARE vclient_id INT;
  
   
   call sp_stg_validate_batch_id(vbatch_id,'adserver','date');
   
   update stg_batch set status = 2, updated_on = now() where id = vbatch_id;

   
   select `client_id` into vclient_id from `stg_batch` where `id`=vbatch_id;
   select ifnull(parent_client_id,id) into vclient_id from dim_client where id=vclient_id;

   
   delete from stg_adserver_date where date is null and batch_id=vbatch_id;
   
   
  -- commented out by Sampath on 2022-07-29 : below logic is being handled at transformation script 
  -- update stg_adserver_date set advertiser = concat(yield_partner,' (EB)')
  -- 	where batch_id = vbatch_id
  --		  and yield_partner != '(Not applicable)';

   

   insert into dim_advertiser (name, api_advertiser_id, created_by, created_at)
     select distinct dat.advertiser, dat.advertiser_id, 0, now()
      from stg_adserver_date dat
     where batch_id = vbatch_id
         and not exists (select 1
                 from dim_advertiser dim
                where dim.index_name=lower(replace(dat.advertiser,' ',''))
                      
                );
   
   update dim_advertiser set parent_advertiser_id=id where parent_advertiser_id is null;

   
   insert into dim_ad_tag_map (data_type,ad_server_id,advertiser_id, client_id, api_ad_unit, api_ad_unit_id, created_by, created_at)
    select data_type, adserver_id, advertiser_id, client_id, ad_unit, ad_unit_id, created_by, created_at
	  from ( select distinct 'adserver' `data_type`, 
          dat.adserver_id,
          adv.id advertiser_id,
          vclient_id `client_id`,
          get_valid_ad_unit_name(dat.ad_unit) `ad_unit`, 
          dat.ad_unit_id, 
          0 created_by, 
          now() created_at 
   from stg_adserver_date dat
        inner join dim_advertiser adv on adv.index_name=lower(replace(dat.advertiser,' ',''))
   where batch_id = vbatch_id
       ) derived
    where not exists (select 1
             from dim_ad_tag_map map 
             where    map.data_type = 'adserver'
                 and map.ad_server_id=derived.adserver_id
                 and map.advertiser_id=derived.advertiser_id
                 and map.client_id=vclient_id
                 and map.api_ad_unit=derived.ad_unit
                
              );

    
   insert into dim_geo (name, created_by, created_at)
     select distinct dat.geo, 0, now()
      from stg_adserver_date dat
     where batch_id = vbatch_id  and dat.geo is not null and dat.geo<>''
         and not exists (select 1
                 from dim_geo dim
                 where dim.name=dat.geo
                );
    
    
   insert into dim_device_category (name, created_by, created_at)
     select distinct dat.device_category, 0, now()
      from stg_adserver_date dat
     where batch_id = vbatch_id and dat.device_category is not null and dat.device_category<>''
         and not exists (select 1
                 from dim_device_category dim
                where dim.index_name=lower(replace(dat.device_category,' ',''))
                );

  
   insert into dim_order (name, api_order_id, created_by, created_at)
     select dat.order, min(dat.order_id), 0, now()
      from stg_adserver_date dat
     where batch_id = vbatch_id
         and not exists (select 1
                 from dim_order dim
                where dim.name=dat.order
                      
                )
	  group by dat.order;

  
   insert into dim_line_item_type (name, created_by, created_at)
     select distinct dat.line_item_type, 0, now()
      from stg_adserver_date dat
     where batch_id = vbatch_id  and dat.line_item_type is not null and dat.line_item_type<>''
         and not exists (select 1
                 from dim_line_item_type dim
                 where dim.name=dat.line_item_type
                );

  
   insert into dim_os (name, created_by, created_at)
     select distinct dat.os, 0, now()
      from stg_adserver_date dat
     where batch_id = vbatch_id  and dat.os is not null and dat.os<>''
         and not exists (select 1
                 from dim_os dim
                 where dim.name=dat.os
                );
    -- added by Sampath on 2022-07-29 : adding new metric 'clicks' to the data flow
   INSERT INTO `fact_adserver_date`
           (`ad_server_id`, 
            `client_id`,
            `date`, 
            `ad_tag_map_id`, 
            `geo_id`, 
            `device_category_id`, 
            `advertiser_id`,
            `order_id`,
            `line_item_type_id`,
            `os_id`,
            `clicks`,
            `total_impressions`, `total_revenue`, `adserver_impressions`,`adserver_revenue`,
            `adsense_impressions`, `adsense_revenue`, `exchange_impressions`, `exchange_revenue`,
            `viewable_impressions`,`measurable_impressions`,
            `updated_at`, `batch_id`
           )
        select dat.adserver_id,
               vclient_id client_id,
               dat.`date`, 
               au.id ad_unit_id, 
               geo.id geo_id, 
               dc.id device_category_id, 
               adv.id advertiser_id,
               ord.id order_id,
               lit.id line_item_type_id,
               os.id os_id, 
               `clicks`,
               `total_impressions`, `total_revenue`, `adserver_impressions`, `adserver_revenue`,
               `adsense_impressions`, `adsense_revenue`, `exchange_impressions`, `exchange_revenue`,
               `viewable_impressions`,`measurable_impressions`,
               now(),vbatch_id
            from stg_adserver_date dat
                 left outer join dim_device_category dc on dat.device_category=dc.name
                 left outer join dim_geo geo on dat.geo=geo.name
                 left outer join dim_advertiser adv on lower(replace(dat.advertiser,' ',''))=adv.index_name 
                                                       
                 left outer join dim_order ord on dat.order=ord.name 
                                                  
                 left outer join dim_line_item_type lit on dat.line_item_type=lit.name 
                 left outer join dim_os os on dat.os=os.name 
                 left outer join dim_ad_tag_map au  on au.data_type='adserver'
                                                   and au.api_ad_unit = replace(replace(replace(replace(replace(replace(replace(replace(replace(dat.ad_unit,'Â',''),'¬â€',''),'¬ª',''),'¬†',' '),' ',' '),'»',' '),'  ',' '),'  ',' '),'  ',' ')
                                                   and au.ad_server_id = dat.adserver_id
                                                   and au.client_id = vclient_id
                                                   and au.advertiser_id = adv.id
                                                   
        where dat.batch_id = vbatch_id
   on duplicate key update 
  `clicks` = values(`clicks`),
  `total_impressions` = values(`total_impressions`), 
  `total_revenue` = values(`total_revenue`), 
  `adserver_impressions` = values(`adserver_impressions`),
  `adserver_revenue` = values(`adserver_revenue`),
  `adsense_impressions` = values(`adsense_impressions`), 
  `adsense_revenue` = values(`adsense_revenue`), 
  `exchange_impressions` = values(`exchange_impressions`), 
  `exchange_revenue` = values(`exchange_revenue`),
  `viewable_impressions` = values(`viewable_impressions`), 
  `measurable_impressions` = values(`measurable_impressions`), 
  `updated_at` = now(), 
  `batch_id` = vbatch_id,
  `version` = `version`+1;

       update stg_batch set status = 90, updated_on = now() where id = vbatch_id;
       
       call sp_refresh_summary_batch_id (vbatch_id);

       INSERT IGNORE INTO `log_fact_table_update`(`table_name`,`date`,`client_id`, `batch_id`) 
			SELECT distinct 'fact_adserver_date', date, vclient_id, vbatch_id from stg_adserver_date where batch_id=vbatch_id;
       
       update stg_batch set status = 99, updated_on = now() where id = vbatch_id;
     
END;

-- separator


Data Source: Ascendeum DS6 DEV
Schema: datasat_9
Routine: sp_stg2fact_adserver_hour


create
    definer = datasat_9@`%` procedure sp_stg2fact_adserver_hour(IN vbatch_id int)
X:BEGIN
   DECLARE vclient_id INT;
   
   
   call sp_stg_validate_batch_id(vbatch_id,'adserver','hour');
   
   update stg_batch set status = 2, updated_on = now() where id = vbatch_id;

   
   select `client_id` into vclient_id from `stg_batch` where `id`=vbatch_id;
   select ifnull(parent_client_id,id) into vclient_id from dim_client where id=vclient_id;

   
   delete from stg_adserver_hour where date is null and batch_id=vbatch_id;

   
   insert into dim_ad_tag_map (data_type,ad_server_id,client_id, api_ad_unit, api_ad_unit_id, created_by, created_at)
    select data_type, adserver_id, client_id, ad_unit, ad_unit_id, created_by, created_at
	  from ( select distinct 'adserver' `data_type`, 
          dat.adserver_id,
          vclient_id `client_id`,
          dat.ad_unit, 
          dat.ad_unit_id, 
          0 created_by, now() created_at 
   from stg_adserver_date dat
   where batch_id = vbatch_id
       ) derived
    where not exists (select 1
             from dim_ad_tag_map map use index(api_lookup_2)
             where    map.data_type = 'adserver'
                 and map.ad_server_id=derived.adserver_id
                 and map.client_id=vclient_id
                 and map.api_ad_unit=derived.ad_unit
                 
              );

    
   insert into dim_advertiser (name, api_advertiser_id, created_by, created_at)
     select distinct dat.advertiser, dat.advertiser_id, 0, now()
      from stg_adserver_hour dat
     where batch_id = vbatch_id
         and not exists (select 1
                 from dim_advertiser dim
                where dim.index_name=lower(replace(dat.advertiser,' ',''))
                      
                );
   
   update dim_advertiser set parent_advertiser_id=id where parent_advertiser_id is null;

  
   insert into dim_order (name, api_order_id, created_by, created_at)
     select distinct dat.order, dat.order_id, 0, now()
      from stg_adserver_hour dat
     where batch_id = vbatch_id
         and not exists (select 1
                 from dim_order dim
                where dim.name=dat.order
                      
                );


     
    REPLACE INTO `fact_adserver_hour`
           (`ad_server_id`, 
            `client_id`,
            `date`, 
            `hour`,
            `ad_tag_map_id`, 
            `advertiser_id`,
            `order_id`, 
            `total_impressions`, `total_revenue`, `adserver_impressions`,`adserver_revenue`,
            `adsense_impressions`, `adsense_revenue`, `exchange_impressions`, `exchange_revenue`,
            `total_code_served`,
            `updated_at`, `batch_id`
           )
        select dat.adserver_id,
               vclient_id,
               dat.`date`, dat.`hour`,
               au.id ad_unit_id, 
               adv.id advertiser_id,
               ord.id order_id,
               `total_impressions`, `total_revenue`, `adserver_impressions`, `adserver_revenue`,
               `adsense_impressions`, `adsense_revenue`, `exchange_impressions`, `exchange_revenue`,
               `total_code_served`, 
               now(),vbatch_id
            from stg_adserver_hour dat
                 left outer join dim_ad_tag_map au use index(api_lookup_2) on au.data_type='adserver'
                                                   and au.api_ad_unit = dat.ad_unit
                                                   and au.ad_server_id = adserver_id
                                                   and au.client_id = vclient_id
                                                   
                 left outer join dim_advertiser adv on lower(replace(dat.advertiser,' ',''))=adv.index_name 
                                                       
                 left outer join dim_order ord on dat.order=ord.name 
                                                  
        where dat.batch_id = vbatch_id;

       INSERT IGNORE INTO `log_fact_table_update`(`table_name`,`date`,`client_id`, `batch_id`) 
			SELECT distinct 'fact_adserver_hour', date, vclient_id, vbatch_id  from stg_adserver_hour where batch_id=vbatch_id;

      update stg_batch set status = 99, updated_on = now() where id = vbatch_id;
     
END;

grant execute on procedure sp_stg2fact_adserver_hour to autowriter_9;

grant execute on procedure sp_stg2fact_adserver_hour to missioncontrol_api;

--  separator


Data Source: Ascendeum DS6 DEV
Schema: datasat_9
Routine: sp_stg2fact_advertiser


create
    definer = datasat_9@`%` procedure sp_stg2fact_advertiser(IN vbatch_id int)
X:BEGIN
   DECLARE vclient_id INT;
   
   call sp_stg_validate_batch_id(vbatch_id,'advertiser','default');
   
   update stg_batch set status = 2, updated_on = now() where id = vbatch_id;

   select `client_id` into vclient_id from `stg_batch` where `id`=vbatch_id;
   select ifnull(parent_client_id,id) into vclient_id from dim_client where id=vclient_id;

   delete from stg_advertiser where date is null and batch_id=vbatch_id;
    
   insert into dim_geo (name, created_by, created_at)
	   select distinct dat.geo, 0, now()
		  from stg_advertiser dat
		 where dat.batch_id=vbatch_id
               and dat.geo is not null 
               and dat.geo<>''
			   and not exists (select 1 
                                 from dim_geo dim  
								where dim.name=dat.geo 
							  );
    
   insert into dim_device_category (name, created_by, created_at)
	   select distinct dat.device_category, 0, now()
		  from stg_advertiser dat
		 where dat.batch_id=vbatch_id
               and dat.device_category is not null 
               and dat.device_category<>''
			   and not exists (select 1
								 from dim_device_category dim
								where dim.index_name=lower(replace(dat.device_category,' ',''))
							  );
    
   insert into dim_advertiser (name, api_advertiser_id, created_by, created_at)
	   select distinct dat.advertiser, null, 0, now()
		  from stg_advertiser dat
		 where batch_id=vbatch_id
			   and not exists (select 1
								 from dim_advertiser dim
								where dim.index_name=lower(replace(dat.advertiser,' ',''))
							  );
   update dim_advertiser set parent_advertiser_id=id where parent_advertiser_id is null;

   DROP TABLE IF exists `tmp_advertiser`;
   CREATE TEMPORARY TABLE `tmp_advertiser`
    AS
    SELECT
        adv.id as advertiser_id, 
        vclient_id as client_id, 
        geo.id as geo_id, 
        dc.id as device_category_id,
        if(dat.ad_unit_id='',null,dat.ad_unit_id) as ad_unit_id,
         get_valid_ad_unit_name(dat.ad_unit) as ad_unit,
        dat.`date` as `date`,
        dat.impressions,
        dat.revenue,
        dat.ad_requests,
        dat.viewable_impressions,
        dat.clicks,
        1 status,
        get_pref_value_cli_adv('duplicate_data_handling',vclient_id,adv.id, dat.ad_unit) `duplicate_data_handling`
     FROM stg_advertiser dat
          left join dim_advertiser adv on adv.index_name=lower(replace(dat.advertiser,' ',''))
          left join dim_geo geo on geo.name=dat.geo
          left join dim_device_category dc on dc.index_name=lower(replace(dat.device_category,' ',''))
	 WHERE batch_id=vbatch_id;

   DROP TABLE IF exists `tmp_advertiser_summary`;
   CREATE TEMPORARY TABLE `tmp_advertiser_summary`
   AS
   select client_id, advertiser_id, ad_unit, date, count(*)
						  from tmp_advertiser 
						group by client_id,advertiser_id,date, ad_unit 
						having count(1) > 1;

    update tmp_advertiser tmp
           inner join tmp_advertiser_summary ref 
							   on  ref.client_id=tmp.client_id 
							   and ref.advertiser_id=tmp.advertiser_id
                               and ref.ad_unit=tmp.ad_unit
                               and ref.date=tmp.date
       set status=2;

   DROP TABLE IF exists `tmp_advertiser_summary`;
   CREATE TEMPORARY TABLE `tmp_advertiser_summary`
	   AS
	   select `advertiser_id`, `client_id`, 
			  any_value(`geo_id`) `geo_id`, 
			  any_value(`device_category_id`) `device_category_id`,
			  any_value(`ad_unit_id`) `ad_unit_id`,
			  `ad_unit`,
			  `date`,
			  sum(`impressions`) `impressions`,
			  sum(`revenue`) `revenue`,
			  sum(`ad_requests`) `ad_requests`,
			  sum(`viewable_impressions`) `viewable_impressions`,
			  sum(`clicks`) `clicks`
		  from tmp_advertiser 
		  where status=2 and `duplicate_data_handling` ='Aggregate'
		 group by client_id,advertiser_id, ad_unit, date;

	INSERT INTO `tmp_advertiser`
			 (`advertiser_id`, `client_id`, `geo_id`, `device_category_id`,
			  `ad_unit_id`,`ad_unit`,
			  `date`,
			  `impressions`,`revenue`,`ad_requests`,`viewable_impressions`,`clicks`,
			  `status`)
		select `advertiser_id`, `client_id`, `geo_id`, `device_category_id`,
			  `ad_unit_id`,`ad_unit`,
			  `date`,
			  `impressions`,`revenue`,`ad_requests`,`viewable_impressions`,`clicks`,
			  1
		  from tmp_advertiser_summary;

     DROP TABLE IF exists `tmp_advertiser_summary`;
	 CREATE TEMPORARY TABLE `tmp_advertiser_summary`
		   AS
			select `advertiser_id`, `client_id`, 
                  any_value(`geo_id`) `geo_id`, 
                  any_value(`device_category_id`) `device_category_id`,
				  any_value(`ad_unit_id`) `ad_unit_id`,
                  `ad_unit`,
				  `date`,
				  any_value(`impressions`) `impressions`,
                  any_value(`revenue`) `revenue`,
                  any_value(`ad_requests`) `ad_requests`,
                  any_value(`viewable_impressions`) `viewable_impressions`,
                  any_value(`clicks`) `clicks`
			  from tmp_advertiser 
              where status=2 and `duplicate_data_handling` = 'IgnoreDuplicates'
			 group by client_id,advertiser_id, ad_unit, date;

   INSERT INTO `tmp_advertiser`
				 (`advertiser_id`, `client_id`, `geo_id`, `device_category_id`,
				  `ad_unit_id`,`ad_unit`,
				  `date`,
				  `impressions`,`revenue`,`ad_requests`,`viewable_impressions`,`clicks`,
				  `status`)
			select `advertiser_id`, `client_id`, `geo_id`, `device_category_id`,
				  `ad_unit_id`,`ad_unit`,
				  `date`,
				  `impressions`,`revenue`,`ad_requests`,`viewable_impressions`,`clicks`,
				  1
			  from tmp_advertiser_summary;

   
   insert into dim_ad_tag_map (data_type,advertiser_id,client_id, api_ad_unit, api_ad_unit_id, site_id, created_by, created_at)
   select distinct 'advertiser', advertiser_id, client_id, ad_unit, null, null, 0, now()
   from `tmp_advertiser` dat
   where not exists (select 1
		  			   from dim_ad_tag_map map
					  where     map.data_type = 'advertiser'
                            and map.advertiser_id=dat.advertiser_id
					        and map.client_id=dat.client_id
						    and map.api_ad_unit=dat.ad_unit
				    );

   INSERT INTO `fact_advertiser`
           (`advertiser_id`,
            `client_id`,
			`date`, 
            `ad_tag_map_id`, 
            `impressions`, `revenue`, `ad_requests`,`viewable_impressions`, `clicks`,
            `updated_at`,`batch_id`
           )
        select dat.advertiser_id,
               vclient_id,
               dat.`date`,
               au.id ad_unit_id, 
               `impressions`, `revenue`, `ad_requests`, `viewable_impressions`,`clicks`,
               now(), vbatch_id
            from `tmp_advertiser` dat
                 left outer join dim_ad_tag_map au on au.data_type='advertiser' 
 							    and dat.ad_unit=au.api_ad_unit
 							    and dat.client_id=au.client_id
                                and dat.advertiser_id=au.advertiser_id
			   where dat.status=1
		   on duplicate key update 
		  `impressions` = values(`impressions`), 
		  `revenue` = values(`revenue`), 
		  `ad_requests` = values(`ad_requests`),
		  `viewable_impressions` = values(`viewable_impressions`),
		  `clicks` = values(`clicks`), 
		  `updated_at` = now(), 
		  `batch_id` = vbatch_id,
		  `version` = `version`+1;
          
       update stg_batch set status = 90, updated_on = now() where id = vbatch_id;
       
       call sp_refresh_summary_batch_id (vbatch_id);

       INSERT IGNORE INTO `log_fact_table_update`(`table_name`,`date`,`client_id`, `batch_id`) 
              SELECT distinct 'fact_advertiser', date, vclient_id, vbatch_id from stg_advertiser where batch_id=vbatch_id;
  
       update stg_batch set status = 99, updated_on = now() where id = vbatch_id;
   
END;

grant execute on procedure sp_stg2fact_advertiser to autowriter_9;

-- separator


Data Source: Ascendeum DS6 DEV
Schema: datasat_9
Routine: sp_stg2fact_buyside


create
    definer = datasat_9@`%` procedure sp_stg2fact_buyside(IN vbatch_id int)
X:BEGIN
   
   DECLARE vclient_id INT;
   
   
   call sp_stg_validate_batch_id(vbatch_id,'buyside','default');
   
UPDATE stg_batch 
SET 
    status = 2,
    updated_on = NOW()
WHERE
    id = vbatch_id;

   
SELECT 
    `client_id`
INTO vclient_id FROM
    `stg_batch`
WHERE
    `id` = vbatch_id;
SELECT 
    IFNULL(parent_client_id, id)
INTO vclient_id FROM
    dim_client
WHERE
    id = vclient_id;

   IF (vclient_id is NULL) THEN 
      SIGNAL SQLSTATE '45009'
      SET MESSAGE_TEXT = 'Client ID cannot be NULL.';
   END IF;

   
DELETE FROM stg_buyside 
WHERE
    date IS NULL AND batch_id = vbatch_id;

   
   insert into dim_buyside_publisher (name, created_by, created_at)
	   select distinct dat.publisher, 0, now()
		  from stg_buyside dat
		 where dat.batch_id=vbatch_id and dat.publisher is not null and dat.publisher<>'' and dat.publisher<>'(not set)'
			   and not exists (select 1
								 from dim_buyside_publisher dim
								 where dim.index_name=lower(replace(dat.publisher,' ',''))
							  );
 
   
   insert into dim_buyside_device (name, created_by, created_at)
	   select distinct dat.device, 0, now()
		  from stg_buyside dat
		 where dat.batch_id=vbatch_id and dat.device is not null and dat.device<>'' and dat.device<>'(not set)'
			   and not exists (select 1
								 from dim_buyside_device dim
								 where dim.index_name=lower(replace(dat.device,' ',''))
							  );
 
   
   insert into dim_buyside_campaign (name, created_by, created_at)
	   select distinct dat.campaign, 0, now()
		  from stg_buyside dat
		 where dat.batch_id=vbatch_id and dat.campaign is not null and dat.campaign<>'' and dat.campaign<>'(not set)'
			   and not exists (select 1
								 from dim_buyside_campaign dim
								 where dim.name=dat.campaign
							  );

	INSERT INTO `fact_buyside`
		(`date`,
		 `client_id`,
		 `publisher_id`,
		 `campaign_id`,
		 `device_id`,
         `amount_spent`,
		 `clicks`,
         `updated_at`,
         `batch_id`)
        select  dat.`date`,
				vclient_id,
                pub.`id`,
                cam.`id`, 
				dc.`id`,
				dat.`amount_spent`,
				dat.`clicks`,
                now(),
                vbatch_id
            from stg_buyside dat
			 left join dim_buyside_publisher pub on lower(replace(dat.publisher,' ',''))=pub.index_name
			 left join dim_buyside_device dc on lower(replace(dat.device,' ',''))=dc.index_name
			 left join dim_buyside_campaign cam on lower(replace(dat.campaign,' ',''))=cam.index_name
     	   where dat.batch_id=vbatch_id
	   on duplicate key update 
		  `amount_spent` = values(`amount_spent`), 
		  `clicks` = values(`clicks`), 
		  `updated_at` = now(), 
		  `batch_id` = vbatch_id,
		  `version` = `version`+1;
   
       INSERT IGNORE INTO `log_fact_table_update`(`table_name`,`date`,`client_id`, `batch_id`) 
			SELECT distinct 'fact_buyside', date, vclient_id, vbatch_id  from stg_buyside where batch_id=vbatch_id;
   
UPDATE stg_batch 
SET 
    status = 99,
    updated_on = NOW()
WHERE
    id = vbatch_id;
END;

grant execute on procedure sp_stg2fact_buyside to autowriter_9;

grant execute on procedure sp_stg2fact_buyside to missioncontrol_api;