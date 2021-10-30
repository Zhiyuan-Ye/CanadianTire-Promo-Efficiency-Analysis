-- DROP TABLE IF EXISTS sales;
-- CREATE TABLE sales
-- (
--   ID int,
--   category VARCHAR(10),
--   date VARCHAR(10),
--   type VARCHAR(10),
--   promo_units int,
--   reg_units int,
--   year int,
--   week int
-- );

-- select * from sales limit 10
;




-- Reformat
DROP TABLE IF EXISTS sales_1;
CREATE TABLE sales_1 as
select
	repeat('0', 4 - length(cast(id as VARCHAR(5))) ) || cast(id as VARCHAR(5)) as id,
category, date, type, promo_units, reg_units, year, week
from sales;

-- select * from sales_1 limit 10;





-- Data Aggreagation Date level change into Year-Week level
DROP TABLE IF EXISTS sales_2;
CREATE TABLE sales_2 as
SELECT
		id, category, year, week, p_count, r_count, type,
		case when type = 'P' then promo_units else 0 end as promo_units,
		case when type = 'R' then reg_units else 0 end as reg_units
FROM
(
	SELECT
		id, category, year, week,
		p_count,
		r_count,
		case when p_count > r_count then 'P' else 'R' end as type,
		promo_units,
		reg_units
	FROM
	(
			SELECT
				id, category, year, week,
				sum(promo_units) as promo_units, sum(reg_units) as reg_units, sum(p_ind) as p_count, sum(r_ind) as r_count
			FROM
				(
					SELECT id, category, year, week, promo_units, reg_units,
						case when type='P' then 1 else 0 end as p_ind,
						case when type='R' then 1 else 0 end as r_ind
					FROM sales_1
				) as a1
			GROUP BY
			 id, category, year, week
	) as a2
) as a3
;
-- select * from sales_1 limit 10;





-- Data Manipulation :Calculate the percentage of promo weeks of each ID for 3 years
DROP TABLE IF EXISTS sales_3;
CREATE TABLE sales_3 as
SELECT
		*
		, CAST(CAST(COUNT(week) FILTER (WHERE type = 'P') OVER (PARTITION BY id) AS DECIMAL(10 ,4)) / CAST(COUNT(week) OVER (PARTITION BY id) AS DECIMAL(10 ,4)) as decimal(10,3)) as promo_pct  -- see what happens when revoming CAST DECIMAL
    , COUNT(*) FILTER (WHERE (promo_units + reg_units) > 900 ) OVER (PARTITION BY id    ORDER BY id, year, week     ROWS 8 PRECEDING) - (CASE WHEN (promo_units + reg_units) > 900 THEN 1 ELSE 0 END) as gt_900_week_count
    , SUM(promo_units + reg_units) OVER (PARTITION BY id ORDER BY year, week ROWS BETWEEN 4 PRECEDING AND 3 FOLLOWING) units_8_week_sum
FROM sales_2
; --where year in ('2017',2018,2019)

-- select * from sales_3 limit 10




-- Data Manipulation: Cannibalization effects analysis

-- Average of Weighted Moving Average of regular sales from
-- previous 4 most recent available regular weeks AND post 4 most recent available regular weeks,
-- based on TYPE_2 = ‘R’ (exclude ‘P’ and ‘D’).

DROP TABLE IF EXISTS sales_4;
CREATE TABLE sales_4 as
SELECT
	*
FROM
(
	SELECT
		 a.*
		 , b.category_p_count
		 , b.category_all_count
		 , b.cat_week_promo_pct
		 , CASE WHEN cat_week_promo_pct >= 0.2 THEN 'D' ELSE 'R' END AS type_2
	FROM
		sales_3 as a
	LEFT JOIN
	(
		SELECT
			 category, year, week, category_p_count, category_all_count
		 , CAST(category_p_count as DECIMAL(10,2)) / CAST(category_all_count as DECIMAL(10,2)) as cat_week_promo_pct
		FROM
			(
				SELECT DISTINCT
							category, year, week
						, COUNT(*) FILTER (WHERE type='P') OVER (PARTITION BY category, year, week) as category_p_count
						, COUNT(*) OVER (PARTITION BY category, year, week) as category_all_count
				FROM sales_3
			) as k
	) as b
	ON a.category = b.category
	AND a.year = b.year
	AND a.week = b.week
	WHERE a.type = 'R'

	UNION ALL

	SELECT
		 a.*
		 , null
		 , null
		 , null
		 , 'P' as type_2
	FROM
		sales_3 as a
	WHERE a.type = 'P'
) as t
ORDER BY id, year, week
;

-- Data Calculation: Estimated regular sales for each promo week
DROP TABLE IF EXISTS sales_5;
CREATE TABLE sales_5 as

WITH step_1 as
(
SELECT
	*
	, row_number() OVER (PARTITION BY id ORDER BY year, week) as id_row_num
FROM sales_4
-- WHERE ID = '0001'
),

step_2 as
(
SELECT
	*
	, CASE WHEN type_2 in ('P', 'D') THEN null ELSE id_row_num END AS lag_1_up_row_num
FROM step_1
),


step_3 as
(
select
	*
	, lag(id_row_num, 1) OVER (PARTITION BY id ORDER BY year, week) as lag_2_up_row_num
	, lag(id_row_num, 2) OVER (PARTITION BY id ORDER BY year, week) as lag_3_up_row_num
	, lag(id_row_num, 3) OVER (PARTITION BY id ORDER BY year, week) as lag_4_up_row_num
	, case when type_2 = 'R' then reg_units else null end as lag_1_up_reg_units
	, lag(reg_units, 1) OVER (PARTITION BY id ORDER BY year, week) as lag_2_up_reg_units
	, lag(reg_units, 2) OVER (PARTITION BY id ORDER BY year, week) as lag_3_up_reg_units
	, lag(reg_units, 3) OVER (PARTITION BY id ORDER BY year, week) as lag_4_up_reg_units

	, lag(id_row_num, 1) OVER (PARTITION BY id ORDER BY year DESC, week DESC) as lag_1_down_row_num
	, lag(id_row_num, 2) OVER (PARTITION BY id ORDER BY year DESC, week DESC) as lag_2_down_row_num
	, lag(id_row_num, 3) OVER (PARTITION BY id ORDER BY year DESC, week DESC) as lag_3_down_row_num
	, lag(id_row_num, 4) OVER (PARTITION BY id ORDER BY year DESC, week DESC) as lag_4_down_row_num
	, lag(reg_units, 1) OVER (PARTITION BY id ORDER BY year DESC, week DESC) as lag_1_down_reg_units
	, lag(reg_units, 2) OVER (PARTITION BY id ORDER BY year DESC, week DESC) as lag_2_down_reg_units
	, lag(reg_units, 3) OVER (PARTITION BY id ORDER BY year DESC, week DESC) as lag_3_down_reg_units
	, lag(reg_units, 4) OVER (PARTITION BY id ORDER BY year DESC, week DESC) as lag_4_down_reg_units
from
  step_2
where
  type_2 = 'R'

UNION ALL

select
	*
	, null
	, null
	, null
	, null
	, null
	, null
	, null
	, null
	, null
	, null
	, null
	, null
	, null
	, null
	, null
from
	step_2
where
	type_2 <> 'R'

),


step_4 as
(
select
	*
	, id_row_num - coalesce(lag_1_up_row_num, max(lag_1_up_row_num) over (partition by id, grp)) as distance_up_1
	, id_row_num - coalesce(lag_2_up_row_num, max(lag_2_up_row_num) over (partition by id, grp)) as distance_up_2
	, id_row_num - coalesce(lag_3_up_row_num, max(lag_3_up_row_num) over (partition by id, grp)) as distance_up_3
	, id_row_num - coalesce(lag_4_up_row_num, max(lag_4_up_row_num) over (partition by id, grp)) as distance_up_4

	, id_row_num - coalesce(lag_1_down_row_num, max(lag_1_down_row_num) over (partition by id, grp)) as distance_down_1
	, id_row_num - coalesce(lag_2_down_row_num, max(lag_2_down_row_num) over (partition by id, grp)) as distance_down_2
	, id_row_num - coalesce(lag_3_down_row_num, max(lag_3_down_row_num) over (partition by id, grp)) as distance_down_3
	, id_row_num - coalesce(lag_4_down_row_num, max(lag_4_down_row_num) over (partition by id, grp)) as distance_down_4

	, coalesce(lag_1_up_reg_units, max(lag_1_up_reg_units) over (partition by id, grp)) as reg_units_1_up
	, coalesce(lag_2_up_reg_units, max(lag_2_up_reg_units) over (partition by id, grp)) as reg_units_2_up
	, coalesce(lag_3_up_reg_units, max(lag_3_up_reg_units) over (partition by id, grp)) as reg_units_3_up
	, coalesce(lag_4_up_reg_units, max(lag_4_up_reg_units) over (partition by id, grp)) as reg_units_4_up

	, coalesce(lag_1_down_reg_units, max(lag_1_down_reg_units) over (partition by id, grp)) as reg_units_1_down
	, coalesce(lag_2_down_reg_units, max(lag_2_down_reg_units) over (partition by id, grp)) as reg_units_2_down
	, coalesce(lag_3_down_reg_units, max(lag_3_down_reg_units) over (partition by id, grp)) as reg_units_3_down
	, coalesce(lag_4_down_reg_units, max(lag_4_down_reg_units) over (partition by id, grp)) as reg_units_4_down

from
		 (select
				*
				, sum(case when lag_1_up_row_num is not null then 1 end) over (partition by id order by id_row_num) as grp
				, sum(case when lag_1_up_row_num is not null then 1 end) over (partition by id rows between UNBOUNDED preceding and current row) as grp2
      from step_3
     ) b
),

-- SELECT id, year, week, type_2, id_row_num, lag_1_up_row_num, lag_2_up_row_num, lag_1_down_row_num, grp, grp2 FROM step_4 ORDER BY id, year, week limit 1000

step_5 as
(
select
	*
	, CASE WHEN (component_up_1 + component_up_2 + component_up_3 + component_up_4) > 0
	       THEN CAST(component_up_1 / (component_up_1 + component_up_2 + component_up_3 + component_up_4) as decimal(10,2)) ELSE 0 END as weight_up_1
	, CASE WHEN (component_up_1 + component_up_2 + component_up_3 + component_up_4) > 0
	       THEN CAST(component_up_2 / (component_up_1 + component_up_2 + component_up_3 + component_up_4) as decimal(10,2)) ELSE 0 END as weight_up_2
	, CASE WHEN (component_up_1 + component_up_2 + component_up_3 + component_up_4) > 0
	       THEN CAST(component_up_3 / (component_up_1 + component_up_2 + component_up_3 + component_up_4) as decimal(10,2)) ELSE 0 END as weight_up_3
	, CASE WHEN (component_up_1 + component_up_2 + component_up_3 + component_up_4) > 0
	       THEN CAST(component_up_4 / (component_up_1 + component_up_2 + component_up_3 + component_up_4) as decimal(10,2)) ELSE 0 END as weight_up_4

	, CASE WHEN (component_down_1 + component_down_2 + component_down_3 + component_down_4) > 0
	       THEN CAST(component_down_1 / (component_down_1 + component_down_2 + component_down_3 + component_down_4) as decimal(10,2)) ELSE 0 END as weight_down_1
	, CASE WHEN (component_down_1 + component_down_2 + component_down_3 + component_down_4) > 0
	       THEN CAST(component_down_2 / (component_down_1 + component_down_2 + component_down_3 + component_down_4) as decimal(10,2)) ELSE 0 END as weight_down_2
	, CASE WHEN (component_down_1 + component_down_2 + component_down_3 + component_down_4) > 0
	       THEN CAST(component_down_3 / (component_down_1 + component_down_2 + component_down_3 + component_down_4) as decimal(10,2)) ELSE 0 END as weight_down_3
  , CASE WHEN (component_down_1 + component_down_2 + component_down_3 + component_down_4) > 0
	       THEN CAST(component_down_4 / (component_down_1 + component_down_2 + component_down_3 + component_down_4) as decimal(10,2)) ELSE 0 END as weight_down_4
from
		(
		select
			*
			, case when (distance_up_1 is null) or (distance_up_1 = 0) then 0 else CAST(1/ CAST(distance_up_1 AS decimal(10,2)) AS decimal(10,2)) end as component_up_1
			, case when (distance_up_2 is null) or (distance_up_2 = 0) then 0 else CAST(1/ CAST(distance_up_2 AS decimal(10,2)) AS decimal(10,2)) end as component_up_2
			, case when (distance_up_3 is null) or (distance_up_3 = 0) then 0 else CAST(1/ CAST(distance_up_3 AS decimal(10,2)) AS decimal(10,2)) end as component_up_3
			, case when (distance_up_4 is null) or (distance_up_4 = 0) then 0 else CAST(1/ CAST(distance_up_4 AS decimal(10,2)) AS decimal(10,2)) end as component_up_4

			, case when (distance_down_1 is null) or (distance_down_1 = 0) then 0 else CAST(1/ CAST(ABS(distance_down_1) AS decimal(10,2)) AS decimal(10,2)) end as component_down_1
			, case when (distance_down_2 is null) or (distance_down_2 = 0) then 0 else CAST(1/ CAST(ABS(distance_down_2) AS decimal(10,2)) AS decimal(10,2)) end as component_down_2
			, case when (distance_down_3 is null) or (distance_down_3 = 0) then 0 else CAST(1/ CAST(ABS(distance_down_3) AS decimal(10,2)) AS decimal(10,2)) end as component_down_3
			, case when (distance_down_4 is null) or (distance_down_4 = 0) then 0 else CAST(1/ CAST(ABS(distance_down_4) AS decimal(10,2)) AS decimal(10,2)) end as component_down_4
		from
			step_4
		) as b
),


step_6 as
(
select
	*
	, CASE WHEN pre_wma is null then post_wma
	       WHEN post_wma is null then pre_wma
		     ELSE CAST((pre_wma + post_wma) / 2 as decimal(10,2))
	  END as estimated_reg_units
from
		(
		select
			*
			, weight_up_1 * reg_units_1_up + weight_up_2 * reg_units_2_up + weight_up_3 * reg_units_3_up + weight_up_4 * reg_units_4_up as pre_wma
			, weight_down_1 * reg_units_1_down + weight_down_2 * reg_units_2_down + weight_down_3 * reg_units_3_down + weight_down_4 * reg_units_4_down as post_wma
		from
			step_5
		) as b
)

SELECT
	*
FROM step_6
;

-- SELECT id, year, week, type_2, pre_wma, post_wma, estimated_reg_units FROM sales_5 ORDER BY id, year, week limit 1000





-- Data Manipulation: peak/off season analysis for product consecutively sales for more than 3 weeks.
DROP TABLE IF EXISTS sales_6;
CREATE TABLE sales_6 as
with t1 as
(
		select
			*
			, MAX(id_row_num) FILTER(where flag=1) OVER (PARTITION BY id ORDER BY  year, week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) b0
		from
		(
			select
							*,
							case when type_2 <> 'P' then 1 else 0 end flag
			from sales_5
		) as b
)

select
	*
	, case when f0=b0 then 0 else f0-b0-1 end as window_size
  , case when (flag = 1 and lag(flag, 1) OVER (ORDER BY id, year desc, week desc) = 0) or (flag = 1 and lag(flag, 1) OVER (ORDER BY id, year, week) = 0) then 1 else 0 end as window_frame
	, case when (flag = 0 and lag(flag, 1) OVER (ORDER BY id, year, week) = 1) then 1
	  else 0
		end as window_start
	, case when (flag = 0 and lag(flag, 1) OVER (ORDER BY id, year desc, week desc) = 1) then 1
    else 0
	  end as window_end
from
(
		select
			*
			, MIN(id_row_num) FILTER(where flag=1) OVER (PARTITION BY id ORDER BY year, week ROWS BETWEEN 0 PRECEDING AND UNBOUNDED FOLLOWING) f0
		from t1
) as b
;






-- Dada Calculation: Lifting the ESTIMATED_REG_UNITS
DROP TABLE IF EXISTS sales_7;
CREATE TABLE sales_7 as
with step_1 as
(
	SELECT
		*
		, reg_units as window_frame_reg_units_up
		, lag(reg_units, 1) over (partition by id order by year DESC, week DESC) as window_frame_reg_units_down
	    , estimated_reg_units as window_frame_estimated_units_up
		, lag(estimated_reg_units, 1) over (partition by id order by year DESC, week DESC) as window_frame_estimated_units_down
	from
		sales_6
	where window_frame=1
	    --and id in ('0005', '0002')

	UNION ALL

	SELECT
		*
		, null
		, null
		, null
		, null
	from
		sales_6
	where window_frame<>1
	 -- and id in ('0005', '0002')
),

step_2 as
(
	select
		*
		, sum(case when window_frame_reg_units_up is not null then 1 end) over (partition by id order by year, week) as grp_2
	from
		step_1
	order by id, year , week
),

step_3 as
(
	select
		*
		, case when window_size>=3 then coalesce(window_frame_reg_units_up, max(window_frame_reg_units_up) over (partition by id, grp_2))
					 else null
		  end as window_frame_reg_units_1
		, case when window_size>=3 then coalesce(window_frame_reg_units_down, max(window_frame_reg_units_down) over (partition by id, grp_2))
					 else null
		  end as window_frame_reg_units_2
		, case when window_size>=3 then coalesce(window_frame_estimated_units_up, max(window_frame_estimated_units_up) over (partition by id, grp_2))
					 else null
		  end as window_frame_estimated_units_1
		, case when window_size>=3 then coalesce(window_frame_estimated_units_down, max(window_frame_estimated_units_down) over (partition by id, grp_2))
					 else null
		  end as window_frame_estimated_units_2
	from
		step_2
),

step_4 as
(
	select
		*
		, cast(
		  case when window_frame_reg_units_1>0 and window_frame_reg_units_2>0 and window_frame_estimated_units_1>0 and window_frame_estimated_units_2>0
		       then (window_frame_reg_units_1 + window_frame_reg_units_2) / (window_frame_estimated_units_1 + window_frame_estimated_units_2)
		  end
			as decimal(10,3)) as up_degree
	from
		step_3
),

step_5 as
(
	select
		*
		, case when up_degree >=1 then up_degree else 1 end as up_degree_2
		, cast(
		  case when window_size>=3 then promo_units / sum(promo_units) over (partition by id, f0)
	    end
			as decimal(10,3)) as promo_weight

	from
		step_4
),

step_6 as
(
	select
		*
		, estimated_reg_units * up_degree_2 as lifted_estimated_reg_units_raw
  from
		step_5
),

step_7 as
(
	select
		*
		, case when window_size>=3 then sum(lifted_estimated_reg_units_raw) filter(where type_2='P') over (partition by id, f0) end as window_sum_lifted_estimated_units_raw
	from
		step_6
),

step_8 as
(
	select
		*
		, case when window_size >= 3 then window_sum_lifted_estimated_units_raw * promo_weight
		  else estimated_reg_units
		  end	as estimated_reg_units_2
	from
		step_7
)


select * from step_8
;