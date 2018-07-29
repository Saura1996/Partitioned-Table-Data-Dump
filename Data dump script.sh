v_table_data_date=$1
if [[ v_table_data_date -eq "" ]]; 
      then echo "Date not passed as parameter";  v_table_data_date=$(date -d '-1 days'  +%Y%m%d); 
      else echo "Date provided" ; 
fi


## initializing the date variables to be used in Big Query Table date range function ($dt) and to name the new table with date suffix ($dt2)

dt=$(date "+2018-05-01")
dt2=$(date "+20180501")

## Creating a loop function to start from i = 0 to i = 79; which accounts to date between 01-05-2018 to 20-07-2018
for i in {0..79}
do
## Loop started

## initializing the dataset names
  v_dataset_name1=project_product_metrics_intermediates;
  v_dataset_name2=project_product_metrics;

## initializing the project name
  v_bq_project_name="big-query-1233";
  v_rollup_dataset_name="124486161";


  date
  echo "$dt"


  ## scheduling_product_metrics_search_discvery_funnel loading. Replacing the existing table if it is present
  v_query_product_metrics_search_discovery_funnel="SELECT 
  a.sessionid sessionid,
  date,
  a.hit hit,
  appversion,
  user_type,
  platform,
  screen,
  pagetype,
  city,
  channel,
  screenlabel,
  navigation,
  search_flag,
  search_type_hit,
  list_name,
  orderid
  FROM(SELECT 
a.sessionid sessionid,
  date,
  a.hit hit,
  appversion,
  user_type,
  platform,
  screen,
  pagetype,
  city,
  channel,
  screenlabel,
  navigation,
  search_flag,
  search_type_hit,
  list_name
FROM(SELECT a.sessionid sessionid,
  date,
  a.hit hit,
  appversion,
  user_type,
  platform,
  screen,
  pagetype,
  city,
  channel,
  screenlabel,
  navigation,
  INTEGER(CASE WHEN navigation = 'search' THEN '1' ELSE '0' END) search_flag ,
  search_type_hit
FROM(SELECT
a.sessionid sessionid,
  date,
  a.hit hit,
  appversion,
  user_type,
  city,
  channel,
  platform,
  screen,
  pagetype,
  screenlabel,
  navigation
FROM(SELECT
  a.sessionid sessionid,
  date,
  a.hit hit,
  appversion,
  user_type,
  city,
  channel,
  platform,
  screen,
  pagetype,
  screenlabel
FROM
(SELECT
  a.sessionid sessionid,
  date,
  a.hit hit,
  platform,
  appversion,
  user_type,
  city,
  channel,
  screen,
  screenlabel
FROM
(SELECT
  CONCAT(fullVisitorId ,STRING(visitStartTime)) sessionid,
  DATE(SEC_TO_TIMESTAMP(visitStartTime+19800)) date,
  hits.sourcePropertyInfo.sourcePropertyDisplayName platform,
  hits.appInfo.appVersion appversion,
  CASE WHEN totals.newVisits = 1 THEN 'new' ELSE 'returning' END user_type,
  geoNetwork.city city,
   channelGrouping channel,
  hits.hitNumber hit,
  hits.appInfo.screenName screen
FROM
  TABLE_DATE_RANGE([big-query-1233:124486161.ga_sessions_], TIMESTAMP('$dt'), TIMESTAMP('$dt'))
WHERE
  hits.type in ('APPVIEW', 'PAGE')
GROUP EACH BY
  1,2,3,4,5,6,7,8,9) a
LEFT JOIN
(SELECT
  sessionid,
  hit,
  screenlabel
 FROM
(SELECT
  CONCAT(fullVisitorId ,STRING(visitStartTime)) sessionid,
  hits.hitNumber hit,
  hits.customDimensions.index index,
  hits.customDimensions.value screenlabel
FROM
  TABLE_DATE_RANGE([big-query-1233:124486161.ga_sessions_], TIMESTAMP('$dt'), TIMESTAMP('$dt'))
WHERE
  hits.type in ('APPVIEW', 'PAGE')
GROUP EACH BY
  1,2,3,4)
WHERE
  index = 22
GROUP EACH BY
  1,2,3) b
ON a.sessionid = b.sessionid AND a.hit = b.hit
GROUP EACH BY
  1,2,3,4,5,6,7,8,9,10) a
LEFT JOIN
(SELECT
  sessionid,
  hit,
  pagetype
 FROM
(SELECT
  CONCAT(fullVisitorId ,STRING(visitStartTime)) sessionid,
  hits.hitNumber hit,
  hits.customDimensions.index index,
  hits.customDimensions.value pagetype
FROM
  TABLE_DATE_RANGE([big-query-1233:124486161.ga_sessions_], TIMESTAMP('$dt'), TIMESTAMP('$dt'))
WHERE
  hits.type in ('APPVIEW', 'PAGE')
GROUP EACH BY
  1,2,3,4)
WHERE
  index = 3
GROUP EACH BY
  1,2,3) b
ON a.sessionid = b.sessionid AND a.hit = b.hit
GROUP EACH BY
  1,2,3,4,5,6,7,8,9,10,11)a
  LEFT JOIN
  (SELECT
  sessionid,
  hit,
  navigation 
 FROM
(SELECT
  CONCAT(fullVisitorId ,STRING(visitStartTime)) sessionid,
  hits.hitNumber hit,
  hits.customDimensions.index index,
  hits.customDimensions.value navigation
FROM
  TABLE_DATE_RANGE([big-query-1233:124486161.ga_sessions_], TIMESTAMP('$dt'), TIMESTAMP('$dt'))
WHERE
  hits.type in ('APPVIEW', 'PAGE')
GROUP EACH BY
  1,2,3,4)
WHERE
  index = 141
GROUP EACH BY
  1,2,3) b
  ON a.sessionid = b.sessionid AND a.hit = b.hit
  GROUP EACH BY 
  1,2,3,4,5,6,7,8,9,10,11,12)a
  LEFT JOIN
  (SELECT
  sessionid,
  hit,
  search_type_hit 
 FROM
(SELECT
  CONCAT(fullVisitorId ,STRING(visitStartTime)) sessionid,
  hits.hitNumber hit,
  hits.customDimensions.index index,
  hits.customDimensions.value search_type_hit
FROM
  TABLE_DATE_RANGE([big-query-1233:124486161.ga_sessions_], TIMESTAMP('$dt'), TIMESTAMP('$dt'))
WHERE
  hits.type in ('APPVIEW', 'PAGE')
GROUP EACH BY
  1,2,3,4)
WHERE
  index = 42
GROUP EACH BY
  1,2,3) b
  ON a.sessionid = b.sessionid AND a.hit = b.hit
  GROUP EACH BY 
  1,2,3,4,5,6,7,8,9,10,11,12,13,14
  )a
  LEFT JOIN
  (SELECT
  sessionid,
  hit,
  list_name
 FROM
(SELECT
  CONCAT(fullVisitorId ,STRING(visitStartTime)) sessionid,
  hits.hitNumber hit,
  hits.customDimensions.index index,
  hits.customDimensions.value list_name
FROM
  TABLE_DATE_RANGE([big-query-1233:124486161.ga_sessions_], TIMESTAMP('$dt'), TIMESTAMP('$dt'))
WHERE
  hits.type in ('APPVIEW', 'PAGE')
GROUP EACH BY
  1,2,3,4)
WHERE
  index = 138
GROUP EACH BY
  1,2,3)b
  ON
  a.sessionid = b.sessionid
 AND
 a.hit = b.hit
  GROUP EACH BY 
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)a
LEFT JOIN
(SELECT CONCAT(fullVisitorId ,STRING(visitStartTime)) sessionid,
hits.transaction.transactionId orderid
FROM
  TABLE_DATE_RANGE([big-query-1233:124486161.ga_sessions_], TIMESTAMP('$dt'), TIMESTAMP('$dt'))
  WHERE
  hits.type in ('APPVIEW', 'PAGE')
  GROUP BY 1,2)b
  ON a.sessionid = b.sessionid
  GROUP EACH BY
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16"

## Query over

## saving the table name and giving the proper destination to the table
tableName=product_metrics_search_discovery_funnel2
v_destination_tbl="$v_dataset_name1.${tableName}_${dt2}";

echo "bq query --maximum_billing_tier 100 --allow_large_results=1  --replace -n 1  --destination_table=$v_destination_tbl \"$v_query_product_metrics_search_discovery_funnel\""
bq query --maximum_billing_tier 100 --allow_large_results=1 --replace -n 0 --destination_table=$v_destination_tbl "$v_query_product_metrics_search_discovery_funnel" &

##initializing the process Ids 

v_first_pid=$!
v_product_metrics_sdl_pids+=" $v_first_pid"

## waiting till the $v_first_pid process is not over
wait $v_first_pid;


## increasing the value of date by 1 day

dt=$(date +%Y-%m-%d -d "$dt + 1 day")
dt2=$(date +%Y%m%d -d "$dt2 + 1 day")
done

## End of loop 




if wait $v_product_metrics_sdl_pids;
      then echo "Successful Execution of code" ;
else echo "Code failed in one or more table loads" ;
fi

date
echo "Execution of code completed for $v_table_data_date"

if wait $v_product_metrics_sdl_pids;
      then v_table_status="Successful Execution of code" ;
else v_table_status="Code failed in one or more table loads" ;
fi

echo "Table refresh status:$v_table_status`date`" | mail -s "$v_table_status" saurabhdeosarkar100@gmail.com

exit 0













































