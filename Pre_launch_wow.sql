--Pre-Launch WOW Query 

with leads as (
    select 
    * 
    , case 
        when holdout_group = 'Control' then 'Control'
        when l.first_delivery_date_c is not null then 'FD' 
        when l.activation_date_c is not null then 'Activated'
        when l.orientation_date_c is not null then 'Oriented'
        when l.criminal_bgc_passed_date_time_c is not null then 'BGC_Pass'
        when l.criminal_bgc_start_date_time_c is not null then 'BGC_Start'
        when l.application_date_c is not null then 'Applied'
        
        else 'else'
      end as stage_in_funnel 
   
    , case 
        when holdout_group = 'Control' then 0
        when l.first_delivery_date_c is not null then 6 
        when l.activation_date_c is not null then 5
        when l.orientation_date_c is not null then 4
        when l.criminal_bgc_passed_date_time_c is not null then 3
        when l.criminal_bgc_start_date_time_c is not null then 2
        when l.application_date_c is not null then 1
         
      end as funnel_order   
    , case 
        when l.first_delivery_date_c <= cast(created_date as date) then 'FD' 
        when l.orientation_date_c <= cast(created_date as date) then 'Orientation'
        when l.activation_date_c <= cast(created_date as date) then 'Activated'
        when cast(l.criminal_bgc_passed_date_time_c as date) <= cast(created_date as date) then 'BGC_Pass'
        when cast(l.criminal_bgc_start_date_time_c as date) <= cast(created_date as date) then 'BGC_Start'
        when l.application_date_c <= cast(created_date as date) then 'Application'
        else 'error'
       
      end as stage_when_recieved 
    , date_trunc(application_date_c , week(monday)) Application_week  
    , CASE
        WHEN ownership_assigned_date_c is not null AND DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),ownership_assigned_date_c, DAY) < 21 
          THEN DATE_ADD(ownership_assigned_date_c, INTERVAL DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),ownership_assigned_date_c, DAY) DAY)
        WHEN ownership_assigned_date_c is not null AND DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),ownership_assigned_date_c, DAY) >= 21 
          THEN DATE_ADD(ownership_assigned_date_c, INTERVAL 21 DAY)
        WHEN ownership_assigned_date_c is null AND DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),lead_received_date_c, DAY) < 21 
          THEN DATE_ADD(lead_received_date_c, INTERVAL DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),lead_received_date_c, DAY) DAY)
        WHEN ownership_assigned_date_c is null AND DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),lead_received_date_c, DAY) >= 21 
          THEN DATE_ADD(lead_received_date_c, INTERVAL 21 DAY)
       END AS ownership_expiration_date
     , CASE
        WHEN ownership_assigned_date_c is not null AND DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),ownership_assigned_date_c, DAY) < 21 
          THEN DATE_ADD(ownership_assigned_date_c, INTERVAL DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),ownership_assigned_date_c, DAY) DAY)
        WHEN ownership_assigned_date_c is not null AND DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),ownership_assigned_date_c, DAY) >= 21 
          THEN DATE_ADD(ownership_assigned_date_c, INTERVAL 21 DAY)
        WHEN ownership_assigned_date_c is null AND DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),lead_received_date_c, DAY) < 21 
          THEN DATE_ADD(lead_received_date_c, INTERVAL DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),lead_received_date_c, DAY) DAY)
        WHEN ownership_assigned_date_c is null AND DATE_DIFF(DATE_ADD(application_date_c, INTERVAL 45 DAY),lead_received_date_c, DAY) >= 21 
          THEN DATE_ADD(lead_received_date_c, INTERVAL 21 DAY)
       END AS ownership_expiration_date   
    , CASE
        WHEN ownership_assigned_date_c is not null THEN ownership_assigned_date_c
        WHEN ownership_assigned_date_c is null THEN lead_received_date_c
       END AS ownership_date_fix
    
      
    from getsaleswarehouse.dbt_production.gsw_lead_task_production l
    where 1=1 
    and holdout_group = 'Treatment'
    and submarket_c in (
         "Trois Rivieres"
        ,"Drummondville, QC"
        ,"Granby, QC"
        ,"Rouyn-Noranda, QC"
        ,"Sept-Iles, QC"
        ,"Shawinigan, QC"
        ,"Sorel-Tracy, QC"
        ,"Val-d'Or, QC"
        ,"Whistler, BC"
        ,"Rimouski, QC"
        ,"Saguenay, QC"
        ,"Victoriaville, QC"
        ,"Joliette, QC"
        ,"Alma, QC"
        ,"Duncan, BC"
        ,"St. Thomas, ON"
        ,"Fredericksburg, TX"
        ,"Owen Sound, ON"
        )
)


, contacts as (
    select
      l.id
    , contact_flag
    , activity_date
    , case when cast(activity_date as timestamp) <= criminal_bgc_passed_date_time_c and contact_flag is true then True end contact_before_BGC
    , case when activity_date <= l.orientation_date_c  and contact_flag is true then True end contact_before_ori
    , case when activity_date <= l.first_delivery_date_c and contact_flag is true then True end contact_before_Fd
    , case 
        when contact_flag is true and cast(activity_date as timestamp) <= l.criminal_bgc_passed_date_time_c then 'Application'
        when contact_flag is true and cast(activity_date as date) <= l.orientation_date_c then 'BGC' 
        when contact_flag is true and cast(activity_date as date) <= l.first_delivery_date_c then 'Orientation'
        when contact_flag is true and cast(activity_date as date) > l.first_delivery_date_c then 'FD'
        end as first_contacted_stage
    , (case when (cast(activity_date as timestamp) between cast(l.application_date_c as timestamp) and criminal_bgc_passed_date_time_c) and contact_flag is true then activity_date end) c_flag_app_BGC   
    , (case when (cast(activity_date as timestamp) between criminal_bgc_passed_date_time_c and cast(l.orientation_date_c as timestamp)) and contact_flag is true then activity_date end) c_flag_BGC_ori
    , (case when (cast(activity_date as timestamp) between cast(l.orientation_date_c as timestamp) and cast(l.first_delivery_date_c as timestamp)) and contact_flag is true then activity_date end) c_flag_ori_fd 
    , (case when contact_flag is true and cast(activity_date as timestamp) <= l.criminal_bgc_passed_date_time_c and cast(activity_date as date) > l.application_date_c then True end) contacted_during_application
    , (case when contact_flag is true and cast(activity_date as date) <= l.orientation_date_c and cast(activity_date as timestamp) > l.criminal_bgc_passed_date_time_c then True end) contacted_during_BGC
    , (case when contact_flag is true and cast(activity_date as date) <= l.first_delivery_date_c and cast(activity_date as date) > l.orientation_date_c then True end) contacted_during_ori
    , (case when contact_flag is true and cast(activity_date as date) > l.first_delivery_date_c then True end) contacted_during_FD
    , (case when contact_flag is true and cast(activity_date as timestamp) <= l.criminal_bgc_passed_date_time_c then True end) contacted_before_BGC
    , (case when contact_flag is true and cast(activity_date as date) <= l.orientation_date_c then True end) contacted_before_ori
    , (case when contact_flag is true and cast(activity_date as date) <= l.first_delivery_date_c then True end) contacted_before_fd
    
  from leads l
  left join getsaleswarehouse.dbt_production.gsw_task_production t  on t.who_id = l.id 
  --group by 1 , 2 , 3 , 4 , 5
)

select 
  submarket_c
, stage_when_recieved  
, stage_in_funnel 
, cast(timestamp_trunc(created_date , day) as date) created_date 
, cast(timestamp_trunc(created_date , week(monday)) as date) created_week
, date_diff(  first_outbound_contact_attempt_date , cast(timestamp_trunc(created_date , day) as date)  , day) as Days_until_attempt
, date_diff( first_contact_date  , cast(timestamp_trunc(created_date , day) as date) , day) as Days_until_contact
, Application_date_c 
, date_trunc(Application_date_c , week(monday) ) as app_week 
, cast(timestamp_trunc(criminal_bgc_start_date_time_c , day ) as date) start
, date_diff(  cast(timestamp_trunc(criminal_bgc_passed_date_time_c , day ) as date ) , cast(l.created_date as date) ,  day) as Day_between_Created_BGC_Pass
, cast(timestamp_trunc(criminal_bgc_passed_date_time_c , day) as date)  pass 
, date_diff(  orientation_date_c , cast(l.created_date as date) , day) as Day_between_Created_Ori
, orientation_date_c
, date_diff( first_delivery_date_c , cast(l.created_date as date) ,  day) as Day_between_Created_Fd
, activation_date_c
, date_diff(activation_date_c , orientation_date_c , day) Day_between_Ori_Activation 
, first_delivery_date_c
, first_outbound_contact_attempt_date
, first_contact_date
, lead_tier_c
, max(cast(c_flag_app_BGC as date)) c_flag_app_BGC
, max(case when (cast(activity_date as timestamp) between criminal_bgc_passed_date_time_c and cast(l.orientation_date_c as timestamp)) and contact_flag is true then activity_date end) c_flag_BGC_ori
, max(cast(c_flag_ori_fd as date)) c_flag_ori_fd
, count(l.id) lead_count
, case 
        when first_contact_date >= l.first_delivery_date_c then 'FD'
        when first_contact_date >= l.orientation_date_c then 'Orientation' 
        when first_contact_date >= cast(l.criminal_bgc_passed_date_time_c as date) then 'BGC'
        when first_contact_date >= l.application_date_c then 'Application'
        when first_contact_date is null then 'Not Contacted'
        else 'error' 
        end as first_contacted_stage
-- added data
, max(contacted_during_application) contacted_during_application
, max(contacted_during_BGC) contacted_during_BGC
, max(case when contact_flag is true and cast(activity_date as date) <= l.first_delivery_date_c and cast(activity_date as date) > l.orientation_date_c then True end) contacted_during_ori
, max(contacted_during_FD) contacted_during_FD
, max(contacted_before_BGC) contacted_before_BGC
, max(case when activity_date <= l.orientation_date_c  and contact_flag is true then True end) contacted_before_ori
, max(contacted_before_fd) contacted_before_fd
from leads l
left join contacts c on l.id = c.id
where cast(timestamp_trunc(created_date , week(monday)) as date) >= date_sub(date_trunc(current_date() , week) , interval 10 week) 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,26
order by  4 desc , 7 desc , 1 asc 

