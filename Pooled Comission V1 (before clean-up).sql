with tasks as (
  select 
    *
  from getsaleswarehouse.dbt_production.gsw_task_production
  
)  

 , leads as ( 
   select 
     *
   from getsaleswarehouse.dbt_production.gsw_lead_task_production l
 )

 , quota as( 
     select 
       task_id
     , owner_id  
     , timestamp_trunc(cast(activity_timestamp as timestamp) , month) task_month 
     , row_number() over (partition by  owner_id,  timestamp_trunc( cast(activity_timestamp as timestamp) , month) order by cast(activity_timestamp as timestamp)) task_count
     , contact_flag 
     from getsaleswarehouse.dbt_production.gsw_task_production
     where contact_flag = true 
     and activity_timestamp >= '2021-01-18'
     
     

 )


, hr_data as( 
  select 
    *
  from getsaleswarehouse.dbt_production.personnel_data
)

  select
      manager
    , sales_rep
    , case 
       when cast(activity_timestamp as timestamp) >= timestamp_sub(current_timestamp , interval 2 day) then 2
       when cast(activity_timestamp as timestamp) >= timestamp_sub(current_timestamp , interval 7 day) then 7 
       when cast(activity_timestamp as timestamp) >= timestamp_sub(current_timestamp , interval 15 day) then 15
       when cast(activity_timestamp as timestamp) >= timestamp_sub(current_timestamp , interval 30 day) then 30 
       else 31
       end task_age
    , t.experiment_tag_c task_experiment_tag_c
    , t.lead_cohort_c task_lead_cohort_c
    , call_disposition
    , case 
        when call_disposition in ( 'Planned Action' , 'Priority Follow-Up' , 'Standard Follow-Up' , 'Deprioritized Follow-Up' , 'Do Not Call' ) then 'Contacted'
        when call_disposition in ( 'No Conversation Follow-up' , 'No Conversation Follow-up' ,'No Answer' ,  'No  Answer' , 'No  Answer - Left VM' , 'Abandon' , 'Sent To Voicemail' , 'Voicemail','No Conversation Follow-Up' , 'Busy'  ,'Requested Callback' , 'No Disposition') then 'No_Contact' end dispo_contact
   -- Fix Quota number HERE!
   , case 
        when date_trunc(activity_timestamp , month) = '2021-01-01' and task_count < 300 then "Quota_lead"
        when date_trunc(activity_timestamp , month) = '2021-01-01' and task_count >= 300 then "Passed_Quota_lead"
        
        when date_trunc(activity_timestamp , month) > '2021-01-01' and task_count < 600 --change this number for quota 
        then "Quota_lead"
        when date_trunc(activity_timestamp , month) > '2021-01-01' and task_count >= 600 --change this number for quota 
        then "Passed_Quota_lead"
     end Quota_Leads
   --
    , case when activity_timestamp >= '2021-01-18' then true end as pool_rolled_out
    , case when l.experiment_tag_c in ( 'oa_pool_dialing_test_20200928' , 'doordash_standard_pool') then true end pooled_task
    , case when date_diff(  l.ownership_assigned_date_c , l.orientation_date_c , day ) < 10  then true end less_than_10_day_apprv
    , max(total_task) total_tasks
    , count(t.task_id) tasks 
    , sum(call_minutes) sum_talk_time
    , sum( case when call_minutes > .116666 then 1 end ) calls_over_7Sec
    , sum(time_diff( cast(case when length(five_9_wrap_time_c) = 8 then five_9_wrap_time_c end as time) , '00:00:00' , second ) / 60)  sum_wrap_min 
    , sum(case when t.contact_flag = True then 1 end) contact_count
    , sum(case when call_attempt_flag = True then 1 end) attempt_count
    , count(distinct t.who_id) lead_count 
    , max(cast(activity_timestamp as timestamp))  max_date
    , sum(date_diff(  case when  call_disposition is not null then activity_timestamp end ,  l.orientation_date_c , day )) task_days_frm_ori 
    , sum(date_diff(  case when  call_disposition is not null then activity_timestamp end , l.application_date_c ,day )) task_days_frm_applied 
    , sum(cast(l.lead_tier_c as int64) ) sum_lead_tier
    , sum(case when l.experiment_tag_c in ( 'oa_pool_dialing_test_20200928' , 'doordash_standard_pool') then 1 end ) pooled_leads 
    , sum(case when date_diff(  case when  call_disposition is not null then activity_timestamp end ,  l.orientation_date_c , day ) < 10  and l.experiment_tag_c in ( 'oa_pool_dialing_test_20200928' , 'doordash_standard_pool')then 1 end ) pooled_and_less_than_10_day_apprv_count
    , sum(case when date_diff(  case when  call_disposition is not null then activity_timestamp end ,  l.orientation_date_c , day ) >= 10  and l.experiment_tag_c in ( 'oa_pool_dialing_test_20200928' , 'doordash_standard_pool')then 1 end ) pooled_and_grtr_or_equal_than_10_day_apprv_count
    , count( distinct ( case when l.first_delivery_date_c is not null then l.id end ) ) as distinct_fd 
    
    from tasks t 
    left join hr_data hr on hr.user_id = t.owner_id
    left join leads l on l.id = t.who_id 
    left join ( 
      select 
         owner_id  
      ,  sum(1)  Total_task 
      from tasks
      group by 1 
      
      ) tt on tt.owner_id = t.owner_id 
     left join quota q on q.task_id = t.task_id  
      
      where manager is not null 
      and call_disposition is not null
      
      
      group by 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 
      order by 1 , 2  
      

