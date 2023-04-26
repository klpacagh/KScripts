import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import boto3
from datetime import date
import csv
import os
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, split, lit, regexp_replace, current_timestamp, expr, udf, concat_ws, concat, substring, explode, arrays_zip
from pyspark.sql.types import StringType, BooleanType, DoubleType, LongType, DateType, IntegerType, StructType, StructField, TimestampType
from helpers import get_db_instances, create_table_dict, addCreatedAndUpdatedAt, write_df_to_s3, write_df_to_redshift, write_to_ttdm_targets

args = getResolvedOptions(sys.argv, ['JOB_NAME','trusted_bucket', 'refined_bucket', 'config_file_path','connection_name','redshift_role_arn','redshift_endpoint','username','redshift_password','db_name','schema_name'])
table_dict = {}

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
todays_date = str(date.today()).replace('-','/')

system_to_db_map = get_db_instances('crossaccount-rtt-bus')

data_dict = create_table_dict(glueContext, args["config_file_path"], args["trusted_bucket"], system_to_db_map, todays_date)

for k,v in data_dict.items():
    data_dict[k] = data_dict[k].cache()
    print(f'{k}', 'count: ', data_dict[k].count())

ticket_registry_submission = data_dict['ticket_registry_registrysubmission']
ticket_registry_submission.createOrReplaceTempView("ticket_reg_submission")
ticket_registry = spark.sql("select email, first as name_first, last as name_last, zipcode as postal_code, 'ticket_registry' as source_system from ticket_reg_submission where email is not null")
ticket_registry.cache()
print("ticket registry df count: ", ticket_registry.count())

shopify_rc_customers = data_dict['shopify_rc_shopify_rc_customers']
shopify_rc_orders = data_dict['shopify_rc_shopify_rc_orders']
shopify_pga_customers = data_dict['shopify_pga_shopify_pga_customers']
shopify_pga_orders = data_dict['shopify_pga_shopify_pga_orders']

shopify_rc_customers.createOrReplaceTempView("rc_cust")
shopify_pga_customers.createOrReplaceTempView("pga_cust")
shopify_rc_customers = spark.sql("with cust as (select id, email, first_name, last_name, explode(addresses) as address_exploded from rc_cust) select id, email, first_name, last_name, address_exploded.address1, address_exploded.address2, address_exploded.city, address_exploded.province as state_region, address_exploded.country, address_exploded.zip from cust")
shopify_pga_customers = spark.sql("with cust as (select id, email, first_name, last_name, explode(addresses) as address_exploded from pga_cust) select id, email, first_name, last_name, address_exploded.address1, address_exploded.address2, address_exploded.city, address_exploded.province as state_region, address_exploded.country, address_exploded.zip from cust")
shopify_customers = shopify_rc_customers.union(shopify_pga_customers)
shopify_rc_orders = shopify_rc_orders.withColumn("new",arrays_zip("line_items.product_id","line_items.quantity","line_items.price","line_items.total_discount", "line_items.title")).withColumn("new",explode("new")).select(col("id").alias("source_order_id"),col("processed_at").alias("order_date"),col("customer.id").alias("customer_id"),col("address1").alias("billing_address_1"),col("address2").alias("billing_address_2"),col("city").alias("billing_city"),col("province").alias("billing_state_region"),col("zip").alias("billing_postal_code"),col("country").alias("billing_country"),col("shipping_address.address1").alias("shipping_address_1"),col("shipping_address.address2").alias("shipping_address_2"),col("shipping_address.city").alias("shipping_city"),col("shipping_address.province").alias("shipping_state_region"),col("shipping_address.country").alias("shipping_country"),col("shipping_address.zip").alias("shipping_postal_code"),"total_discounts","total_price","total_tax",col("amount").alias("total_ship"), col("subtotal_price").alias("total_gross_sub"),lit('ryder_cup').alias('source_instance'),col("new.0").alias("product_id"), col("new.1").alias("quantity"), col("new.2").alias("price"),col("new.3").alias("total_discount"),col("new.4").alias("title"))
shopify_pga_orders = shopify_pga_orders.withColumn("new",arrays_zip("line_items.product_id","line_items.quantity","line_items.price","line_items.total_discount", "line_items.title")).withColumn("new",explode("new")).select(col("id").alias("source_order_id"),col("processed_at").alias("order_date"),col("customer.id").alias("customer_id"),col("address1").alias("billing_address_1"),col("address2").alias("billing_address_2"),col("city").alias("billing_city"),col("province").alias("billing_state_region"),col("zip").alias("billing_postal_code"),col("country").alias("billing_country"),col("shipping_address.address1").alias("shipping_address_1"),col("shipping_address.address2").alias("shipping_address_2"),col("shipping_address.city").alias("shipping_city"),col("shipping_address.province").alias("shipping_state_region"),col("shipping_address.country").alias("shipping_country"),col("shipping_address.zip").alias("shipping_postal_code"),"total_discounts","total_price","total_tax",col("amount").alias("total_ship"), col("subtotal_price").alias("total_gross_sub"),lit('pga').alias('source_instance'),col("new.0").alias("product_id"), col("new.1").alias("quantity"), col("new.2").alias("price"),col("new.3").alias("total_discount"),col("new.4").alias("title"))
shopify_orders = shopify_rc_orders.union(shopify_pga_orders)
shopify_customers.createOrReplaceTempView("shop_customers")
shopify_orders.createOrReplaceTempView("shop_orders")
shopify = spark.sql("select sc.id as customer_id, so.source_order_id, so.order_date,sc.email, sc.first_name, sc.last_name, 'shopify' as source_system, so.source_instance, sc.address1, sc.address2, sc.city, sc.state_region, sc.country, sc.zip, so.billing_address_1, so.billing_address_2, so.billing_city, so.billing_state_region, so.billing_country, so.billing_postal_code, so.shipping_address_1, so.shipping_address_2, so.shipping_city, so.shipping_state_region, so.shipping_country, so.shipping_postal_code, so.total_discounts, so.total_price, so.total_tax, so.total_ship, so.total_gross_sub, so.product_id, so.quantity, so.price, so.total_discount, so.title from shop_customers sc left join shop_orders so on sc.id = so.customer_id")
shopify.cache()
print("shopify df count: ", shopify.count())

email_subs = data_dict['iterable_api_iterable_api_email_subscribes']
email_unsubs = data_dict['iterable_api_iterable_api_email_unsubscribes']
campaigns = data_dict['iterable_api_iterable_api_campaigns']
data_dict['iterable_api_iterable_api_email_clicks'].createOrReplaceTempView('clicks')
data_dict['iterable_api_iterable_api_users'].createOrReplaceTempView('users')
data_dict['iterable_api_iterable_api_message_types'].createOrReplaceTempView('types')
data_dict['iterable_api_iterable_api_email_opens'].createOrReplaceTempView('opens')
data_dict['iterable_api_iterable_api_lists'].createOrReplaceTempView('lists')
data_dict['iterable_api_iterable_api_email_subscribes'].createOrReplaceTempView('email_subs')
data_dict['iterable_api_iterable_api_email_unsubscribes'].createOrReplaceTempView('email_unsubs')
data_dict['iterable_api_iterable_api_campaigns'].createOrReplaceTempView('email_campaigns')
email_subs = spark.sql("select *, explode(emailListIds) as list_id from email_subs")
email_unsubs = spark.sql("select *, explode(emailListIds) as list_id from email_unsubs")
email_campaigns = spark.sql("select *, explode(listIds) as list_id from email_campaigns")
email_subs.createOrReplaceTempView('subs')
email_unsubs.createOrReplaceTempView('unsubs')
email_campaigns.createOrReplaceTempView('campaigns')
iterable_api_click_open = spark.sql('with email_click_open AS (select email, campaignId as source_campaign_id, "email_click" as marketing_action_type, templateId as template_id, null as proxy_source, ip as consumer_IP, url as click_url, messageId as message_id, createdAt as created_at from clicks UNION select email, campaignId as campaign_id, "email_open" as marketing_action_type, templateId as template_id, proxySource as proxy_source, null as consumer_IP, null as click_url, messageId as message_id, createdAt as created_at from opens) select c.id as source_campaign_id, c.name as campaign_name, c.templateId, c.messageMedium as message_medium, c.createdByUserId as created_by_user_id, c.updatedByUserId as updated_by_user_id, c.campaignState as campaign_state, c.type as campaign_type, u.email, eco.marketing_action_type, c.templateId as template_id, eco.proxy_source, eco.consumer_IP as consumer_ip, eco.click_url, msg.name as message_name, msg.subscriptionPolicy as subscription_policy, u.firstname as name_first, u.lastname as name_last, u.address1 as address_1, u.address2 as address_2, u.city, u.stateRegion as state_region, u.country, u.postalCode as postal_code, u.totalTicketSpend as total_ticket_spend, u.signupDate as sign_up_at, u.signupSource as sign_up_source, li.id as list_id, li.name as list_name, li.description, li.listType as list_type, eco.created_at from users u left join email_click_open eco on u.email = eco.email full outer join campaigns c on eco.source_campaign_id = c.id full outer join lists li on li.id = c.list_id left join types msg on eco.message_id = msg.id')
iterable_api_sub_unsub = spark.sql('with email_sub_unsub AS (select email, list_id, "email_subscribe" as marketing_action_type, createdAt as created_at from subs union select email, list_id, "email_unsubscribe" as marketing_action_type, createdAt as created_at from unsubs) select null as source_campaign_id, null as campaign_name, null as templateId, null as message_medium, null as created_by_user_id, null as updated_by_user_id, null as campaign_state, null as campaign_type, es.email, es.marketing_action_type, null as template_id, null as proxy_source, null as consumer_ip, null as click_url, null as message_name, null as subscription_policy, u.firstname as name_first, u.lastname as name_last, u.address1 as address_1, u.address2 as address_2, u.city, u.stateRegion as state_region, u.country, u.postalCode as postal_code, u.totalTicketSpend as total_ticket_spend, u.signupDate as sign_up_at, u.signupSource as sign_up_source, es.list_id, li.name as list_name, li.description, li.listType as list_type, es.created_at from lists li left join email_sub_unsub es on li.id=es.list_id left join users u on es.email=u.email')
iterable_api_click_open.createOrReplaceTempView("click_open")
iterable_api_sub_unsub.createOrReplaceTempView("sub_unsub")
iterable_api = spark.sql("(select *, 'iterable_api' as source_system from click_open) union (select *, 'iterable_api' as source_system from sub_unsub)")
iterable_api.cache()
print("iterable_api df count: ", iterable_api.count())

data_dict['golf_genius_golf_genius_master_roster'].createOrReplaceTempView('golf_genius')
roster = spark.sql("select id, 'golf_genius' as source_system, email, first_name, last_name, handicap_index, country, state, zip_postal_code, date_of_birth, gender, address_1, address_2, city, team_name from golf_genius") 
roster.cache()
print("roster df count: ", roster.count())

data_dict['splash_that_splash_that_group_contacts'].createOrReplaceTempView("st_group_contacts")
data_dict['splash_that_splash_that_events'].createOrReplaceTempView("st_events")
data_dict['splash_that_splash_that_event_details'].createOrReplaceTempView("st_event_details")
splash_main = spark.sql("select c.id as contact_id, c.first_name, c.last_name, c.primary_email, e.title as event_name, e.id as source_event_id, e.description as event_description, e.event_start as start_time, e.event_end as end_time, e.venue_name, e.rsvp_open as rsvp_open, e.rsvp_closed_at as rsvp_closed_at, e.id as event_id, c.date_rsvped, c.attending, c.checked_in, c.vip, c.waitlist as consumer_waitlist, e.wait_list as event_waitlist, e.created_at, e.modified_at, e.address, e.city, e.state, e.zip_code, e.country, 'splash_that' as source_system from st_group_contacts c left join st_events e on c.event_id = e.id")
data_dict['axs_order_tickets'].createOrReplaceTempView('order_tickets')
data_dict['axs_orders'].createOrReplaceTempView('orders')
data_dict['axs_customers'].createOrReplaceTempView('customers')
data_dict['axs_events'].createOrReplaceTempView('events')
data_dict['axs_payments'].createOrReplaceTempView('payments')
data_dict['axs_venues'].createOrReplaceTempView('venues')
axs = spark.sql("select c.customer_unique_id, c.first_name as name_first, c.last_name as name_last, c.email, c.address1 as address_1, c.address2 as address_2, c.city, c.state as state_region, c.zip as postal_code, c.country, p.address1 as billing_address_1, p.address2 as billing_address_2, p.city as billing_city, p.state as billing_state_region, p.zip as billing_postal_code, p.country as billing_country, c.birthday as birth_date, c.is_opt_in as email_opt_in, o.order_unique_id as source_order_id, ot.product_unique_id as source_product_id, ot.amount_gross as unit_price, o.order_amount as total, o.created_on as order_date, ot.order_ticket_unique_id, ot.event_unique_id, e.event_name, e.event_datetime, v.venue_name, v.venue_address1 as venue_address_1, v.venue_address2 as venue_address_2, v.venue_city, v.venue_state as venue_state_region, v.venue_zip as venue_postal_code, v.venue_country, 'axs' as source_system from customers c left join orders o on c.customer_unique_id = o.customer_unique_id left join payments p on o.order_unique_id = p.order_unique_id left join order_tickets ot on ot.order_unique_id = o.order_unique_id left join events e on e.event_unique_id = ot.event_unique_id left join venues v on v.venue_unique_id = e.venue_unique_id")
axs.cache()
print("axs df count: ", axs.count())

data_dict['salesforce-career-services_salesforce-career-services-contact'].createOrReplaceTempView('salesforce_contact')
sf_career_services = spark.sql("select Id, 'salesforce_career_services' as source_system, trim(BOTH ' \t' FROM Email) as Email, Phone, HomePhone, MobilePhone, FirstName, LastName, Birthdate, split(MailingStreet,',')[0] as Address1, split(MailingStreet,',')[1] as Address2, MailingCity, MailingState, MailingCountry, MailingPostalCode, Gender__c as Gender from salesforce_contact")
sf_career_services.cache()
print("salesforce career services df count: ", sf_career_services.count())

data_dict['salesforce-championships_salesforce-championships-championship'].createOrReplaceTempView("championships")
data_dict['salesforce-championships_salesforce-championships-lead'].createOrReplaceTempView("lead")
data_dict['salesforce-championships_salesforce-championships-opportunity'].createOrReplaceTempView("opportunity")
data_dict['salesforce-championships_salesforce-championships-contact'].createOrReplaceTempView("contact")
data_dict['salesforce-championships_salesforce-championships-account'].createOrReplaceTempView("account")
data_dict['salesforce-championships_salesforce-championships-opportunitylineitem'].createOrReplaceTempView("opplineitem")
sf_champs = spark.sql("WITH full_customers AS (SELECT c.id, c.firstname as name_first, c.lastname as name_last, c.email, c.title as role, split(mailingstreet,',')[0] as Address1, split(mailingstreet,',')[1] as Address2, c.mailingcity as city, c.mailingstate as state_region, c.mailingpostalcode as postal_code, c.mailingcountry as country FROM contact c UNION SELECT null as id, l.firstname as name_first, l.lastname as name_last, l.email, l.title as role, split(street,',')[0] as Address1, split(street,',')[1] as Address2, l.city as city, l.state as state_region, l.postalcode as postal_code, l.country as country FROM lead l WHERE l.isconverted = 'false') SELECT fc.id as source_consumer_id, fc.name_first, fc.name_last, fc.email, fc.Address1, fc.Address2, fc.city, fc.state_region, fc.postal_code, fc.country, fc.role as title, c.birthdate as birth_date, c.gender__c as gender, c.accountid, a.name AS org_name, a.parentid as org_parent_id, 'salesforce_championships' as source_system, oli.name as product_name, oli.listPrice as unit_price, oli.pricebookentryid as source_product_id, oli.quantity, o.id AS source_order_id, o.amount as total, o.closedate as order_date, ch.name as event_name, ch.club_name__c as venue_name, ch.street__c as venue_address_1, ch.city__c as venue_city, ch.state__c as venue_state_region, ch.zip_postal_code__c as venue_postal_code, ch.country__c as venue_country, ch.start_date__c as start_time, ch.end_date__c as end_time FROM full_customers fc LEFT JOIN contact c ON c.id = fc.id LEFT JOIN account a ON c.accountid = a.id FULL OUTER JOIN opportunity o ON o.contactid = c.id LEFT JOIN opplineitem oli on oli.OpportunityId = o.Id LEFT JOIN championships ch ON o.championship__c = ch.id WHERE o.stagename = 'Won'")
sf_champs = sf_champs.cache()
print("salesforce championships df count: ", sf_champs.count())

data_dict['salesforce-reach_salesforce-reach-account'].createOrReplaceTempView('account')
data_dict['salesforce-reach_salesforce-reach-contact'].createOrReplaceTempView('contact')
data_dict['salesforce-reach_salesforce-reach-opportunity'].createOrReplaceTempView('opportunity')
sf_reach = spark.sql("SELECT c.id, c.firstname as name_first, c.lastname as name_last, c.email, split(mailingstreet,',')[0] as Address1, split(mailingstreet,',')[1] as Address2, c.mailingcity as city, c.mailingstate as state_region, c.mailingpostalcode as postal_code, c.mailingcountry as country, c.birthdate as birth_date, c.gender__c as gender, 'salesforce_reach' as source_system, o.id AS source_order_id, o.amount as total, o.closedate as order_date FROM contact c LEFT JOIN account a ON c.accountid = a.id FULL OUTER JOIN opportunity o ON o.contactid = c.id WHERE o.iswon = 'true'")
sf_reach = sf_reach.cache()
print("salesforce reach df count: ", sf_reach.count())

data_dict['coach_tools_contacts'].createOrReplaceTempView('contacts')
data_dict['coach_tools_coach_connections'].createOrReplaceTempView('connections')
data_dict['coach_tools_demographic_profiles'].createOrReplaceTempView('profiles')
data_dict['coach_tools_representatives'].createOrReplaceTempView('representatives')
data_dict['coach_tools_students'].createOrReplaceTempView('students')
coach = spark.sql("select a.id, 'coach_tools' as source_system, trim(BOTH ' \t' FROM a.email) as email, a.phone_number, a.address1, a.address2, a.city, a.state, a.zip, b.first_name, b.last_name, d.gender, case when e.relationship_type = 'PARENT' then TRUE else FALSE end juniorParent from contacts a left join students b on trim(BOTH ' \t' FROM a.email) = trim(BOTH ' \t' FROM b.email) left join connections c on b.id = c.student_id left join profiles d on c.student_id = d.student_id left join representatives e on a.id = e.contact_id")
coach = coach.cache()
print("coach df count: ",coach.count())

data_dict['ccms_evt_reg_tourn_info'].createOrReplaceTempView('tournaments')
data_dict['ccms_cen_cust_phone'].createOrReplaceTempView('phone')
data_dict['ccms_cen_cust_cyber'].createOrReplaceTempView('cyber')
data_dict['ccms_cen_cust_addr'].createOrReplaceTempView('address')
data_dict['ccms_cen_cust_mast'].createOrReplaceTempView('master')
data_dict['ccms_evt_reg_hdr'].createOrReplaceTempView('hdr')
ccms = spark.sql("select a.cust_id, 'ccms' as source_system, a.first_nm, a.last_nm, a.birth_dt,a.sex, b.street1, b.street2, b.street3, b.street4, b.city_nm, b.state_cd, b.country_nm, b.postal_cd,  trim(BOTH ' \t' FROM c.cyber_txt) as cyber_txt, d.phone_num, f.home_course, f.HANDICAP from master a left join address b on a.cust_id = b.cust_id left join cyber c on b.cust_id = c.cust_id left join phone d on c.cust_id = d.cust_id left join hdr e on d.cust_id = e.cust_id left join tournaments f on e.REGI_SERNO = f.REGI_SERNO")
ccms = ccms.cache()
print("ccms df count: ", ccms.count())

r_consumers = roster.select(col('id').cast(StringType()).alias('source_consumer_id'),
             col('source_system'),
             col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             lit(None).cast(BooleanType()).alias('do_not_sell'),
             lit(None).cast(DoubleType()).alias('total_ticket_spend'),
             lit(None).cast(DoubleType()).alias('total_merch_spend'),
             lit(None).cast(DoubleType()).alias('total_pga_spend'),
             lit(None).cast(BooleanType()).alias('secondary_purchaser'),
             lit(None).cast(BooleanType()).alias('secondary_seller'),
             lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
             lit(None).cast(BooleanType()).alias('travel_purchaser'),
             lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
             lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
             lit(None).cast(BooleanType()).alias('internal'),
             lit(None).cast(BooleanType()).alias('vip_status'),
             lit(None).cast(StringType()).alias('pga_professional_id'),
             col('handicap_index').cast(IntegerType()).alias('golf_handicap'),
             lit(None).cast(StringType()).alias('dma_region'),
             lit(None).cast(StringType()).alias('course_home'),
             lit(None).cast(BooleanType()).alias('has_coach'),
             lit(None).cast(StringType()).alias('volunteer_history'),
             lit(None).cast(TimestampType()).alias('sign_up_at'),
             lit(None).cast(StringType()).alias('sign_up_source'),
             col('date_of_birth').cast(DateType()).alias('birth_date'),
             col('gender').cast(StringType()),
             lit(None).cast(BooleanType()).alias('parent_of_junior'),
             lit(None).cast(StringType()).alias('league'),
             col('team_name').cast(StringType()).alias('team'),
             col('address_1'),
             col('address_2'),
             col('city'),
             col('state'),
             col('country'),
             col('zip_postal_code').alias('postal_code'))

sm_consumers = splash_main.select(col("contact_id").cast(StringType()).alias('source_consumer_id'),
               col('source_system'),
             col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('primary_email').cast(StringType()).alias('email'),
             lit(None).cast(BooleanType()).alias('do_not_sell'),
             lit(None).cast(DoubleType()).alias('total_ticket_spend'),
             lit(None).cast(DoubleType()).alias('total_merch_spend'),
             lit(None).cast(DoubleType()).alias('total_pga_spend'),
             lit(None).cast(BooleanType()).alias('secondary_purchaser'),
             lit(None).cast(BooleanType()).alias('secondary_seller'),
             lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
             lit(None).cast(BooleanType()).alias('travel_purchaser'),
             lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
             lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
             lit(None).cast(BooleanType()).alias('internal'),
             lit(None).cast(BooleanType()).alias('vip_status'),
             lit(None).cast(StringType()).alias('pga_professional_id'),
             lit(None).cast(IntegerType()).alias('golf_handicap'),
             lit(None).cast(StringType()).alias('dma_region'),
             lit(None).cast(StringType()).alias('course_home'),
             lit(None).cast(BooleanType()).alias('has_coach'),
             lit(None).cast(StringType()).alias('volunteer_history'),
             lit(None).cast(TimestampType()).alias('sign_up_at'),
             lit(None).cast(StringType()).alias('sign_up_source'),
             lit(None).cast(DateType()).alias('birth_date'),
             lit(None).cast(StringType()).alias('gender'),
             lit(None).cast(BooleanType()).alias('parent_of_junior'),
             lit(None).cast(StringType()).alias('league'),
             lit(None).cast(StringType()).alias('team'),
             col('address').alias('address_1'),
             lit(None).alias('address_2'),
             col('city'),
             col('state'),
             col('country'),
             col('zip_code').alias('postal_code'))

a_consumers = axs.select(col("customer_unique_id").cast(StringType()).alias('source_consumer_id'),
             col('source_system'),
             col('name_first').cast(StringType()).alias('name_first'),
             col('name_last').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             lit(None).cast(BooleanType()).alias('do_not_sell'),
             lit(None).cast(DoubleType()).alias('total_ticket_spend'),
             lit(None).cast(DoubleType()).alias('total_merch_spend'),
             lit(None).cast(DoubleType()).alias('total_pga_spend'),
             lit(None).cast(BooleanType()).alias('secondary_purchaser'),
             lit(None).cast(BooleanType()).alias('secondary_seller'),
             lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
             lit(None).cast(BooleanType()).alias('travel_purchaser'),
             lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
             lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
             lit(None).cast(BooleanType()).alias('internal'),
             lit(None).cast(BooleanType()).alias('vip_status'),
             lit(None).cast(StringType()).alias('pga_professional_id'),
             lit(None).cast(IntegerType()).alias('golf_handicap'),
             lit(None).cast(StringType()).alias('dma_region'),
             lit(None).cast(StringType()).alias('course_home'),
             lit(None).cast(BooleanType()).alias('has_coach'),
             lit(None).cast(StringType()).alias('volunteer_history'),
             lit(None).cast(TimestampType()).alias('sign_up_at'),
             lit(None).cast(StringType()).alias('sign_up_source'),
             col('birth_date').cast(DateType()).alias('birth_date'),
             lit(None).cast(StringType()).alias('gender'),
             lit(None).cast(BooleanType()).alias('parent_of_junior'),
             lit(None).cast(StringType()).alias('league'),
             lit(None).cast(StringType()).alias('team'),
             col('address_1'),
             col('address_2'),
             col('city'),
             col('state_region'),
             col('country'),
             col('postal_code'))

sfcs_consumers = sf_career_services.select(col('Id').cast(StringType()).alias('source_consumer_id'),
               col('source_system'),
             col('FirstName').cast(StringType()).alias('name_first'),
             col('LastName').cast(StringType()).alias('name_last'),
             col('Email').cast(StringType()).alias('email'),
             lit(None).cast(BooleanType()).alias('do_not_sell'),
             lit(None).cast(DoubleType()).alias('total_ticket_spend'),
             lit(None).cast(DoubleType()).alias('total_merch_spend'),
             lit(None).cast(DoubleType()).alias('total_pga_spend'),
             lit(None).cast(BooleanType()).alias('secondary_purchaser'),
             lit(None).cast(BooleanType()).alias('secondary_seller'),
             lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
             lit(None).cast(BooleanType()).alias('travel_purchaser'),
             lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
             lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
             lit(None).cast(BooleanType()).alias('internal'),
             lit(None).cast(BooleanType()).alias('vip_status'),
             lit(None).cast(StringType()).alias('pga_professional_id'),
             lit(None).cast(IntegerType()).alias('golf_handicap'),
             lit(None).cast(StringType()).alias('dma_region'),
             lit(None).cast(StringType()).alias('course_home'),
             lit(None).cast(BooleanType()).alias('has_coach'),
             lit(None).cast(StringType()).alias('volunteer_history'),
             lit(None).cast(TimestampType()).alias('sign_up_at'),
             lit(None).cast(StringType()).alias('sign_up_source'),
             lit(None).cast(DateType()).alias('birth_date'),
             col('Gender').cast(StringType()).alias('gender'),
             lit(None).cast(BooleanType()).alias('parent_of_junior'),
             lit(None).cast(StringType()).alias('league'),
             lit(None).cast(StringType()).alias('team'),
             col('Address1'),
             col('Address2'),
             col('MailingCity'),
             col('MailingState'),
             col('MailingCountry'),
             col('MailingPostalCode'))
                                 
sfc_consumers = sf_champs.select(col('source_consumer_id'),
            col('source_system'),
            col('name_first'),
            col('name_last'),
            col('email'),
            lit(None).cast(BooleanType()).alias('do_not_sell'),
            lit(None).cast(DoubleType()).alias('total_ticket_spend'),
            lit(None).cast(DoubleType()).alias('total_merch_spend'),
            lit(None).cast(DoubleType()).alias('total_pga_spend'),
            lit(None).cast(BooleanType()).alias('secondary_purchaser'),
            lit(None).cast(BooleanType()).alias('secondary_seller'),
            lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
            lit(None).cast(BooleanType()).alias('travel_purchaser'),
            lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
            lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
            lit(None).cast(BooleanType()).alias('internal'),
            lit(None).cast(BooleanType()).alias('vip_status'),
            lit(None).cast(StringType()).alias('pga_professional_id'),
            lit(None).cast(IntegerType()).alias('golf_handicap'),
            lit(None).cast(StringType()).alias('dma_region'),
            lit(None).cast(StringType()).alias('course_home'),
            lit(None).cast(BooleanType()).alias('has_coach'),
            lit(None).cast(StringType()).alias('volunteer_history'),
            lit(None).cast(TimestampType()).alias('sign_up_at'),
            lit(None).cast(StringType()).alias('sign_up_source'),
            lit(None).cast(DateType()).alias('birth_date'),
            col('gender').cast(StringType()).alias('gender'),
            lit(None).cast(BooleanType()).alias('parent_of_junior'),
            lit(None).cast(StringType()).alias('league'),
            lit(None).cast(StringType()).alias('team'),
            col('Address1').alias('address_1'),
            col('Address2').cast(StringType()).alias('address_2'),
            col('city'),
            col('state_region'),
            col('country'),
            col('postal_code'))

sfr_consumers = sf_reach.select(col('id').cast(StringType()).alias('source_consumer_id'),
            col('source_system'),
            col('name_first'),
            col('name_last'),
            col('email'),
            lit(None).cast(BooleanType()).alias('do not sell'),
            lit(None).cast(DoubleType()).alias('total_ticket_spend'),
            lit(None).cast(DoubleType()).alias('total_merch_spend'),
            lit(None).cast(DoubleType()).alias('total_pga_spend'),
            lit(None).cast(BooleanType()).alias('secondary_purchaser'),
            lit(None).cast(BooleanType()).alias('secondary_seller'),
            lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
            lit(None).cast(BooleanType()).alias('travel_purchaser'),
            lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
            lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
            lit(None).cast(BooleanType()).alias('internal'),
            lit(None).cast(BooleanType()).alias('vip_status'),
            lit(None).cast(StringType()).alias('pga_professional_id'),
            lit(None).cast(IntegerType()).alias('golf_handicap'),
            lit(None).cast(StringType()).alias('dma_region'),
            lit(None).cast(StringType()).alias('course_home'),
            lit(None).cast(BooleanType()).alias('has_coach'),
            lit(None).cast(StringType()).alias('volunteer_history'),
            lit(None).cast(TimestampType()).alias('sign_up_at'),
            lit(None).cast(StringType()).alias('sign_up_source'),
            col('birth_date'),
            col('gender').cast(StringType()).alias('gender'),
            lit(None).cast(BooleanType()).alias('parent_of_junior'),
            lit(None).cast(StringType()).alias('league'),
            lit(None).cast(StringType()).alias('team'),
            col('Address1'),
            col('Address2'),
            col('city'),
            col('state_region'),
            lit(None).alias('country'),
            col('postal_code'))


ct_consumers = coach.select(col('id').cast(StringType()).alias('source_consumer_id'),
            col('source_system'),
            col('first_name').cast(StringType()).alias('name_first'),
            col('last_name').cast(StringType()).alias('name_last'),
            col('email').cast(StringType()).alias('email'),
            lit(None).cast(BooleanType()).alias('do_not_sell'),
            lit(None).cast(DoubleType()).alias('total_ticket_spend'),
            lit(None).cast(DoubleType()).alias('total_merch_spend'),
            lit(None).cast(DoubleType()).alias('total_pga_spend'),
            lit(None).cast(BooleanType()).alias('secondary_purchaser'),
            lit(None).cast(BooleanType()).alias('secondary_seller'),
            lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
            lit(None).cast(BooleanType()).alias('travel_purchaser'),
            lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
            lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
            lit(None).cast(BooleanType()).alias('internal'),
            lit(None).cast(BooleanType()).alias('vip_status'),
            lit(None).cast(StringType()).alias('pga_professional_id'),
            lit(None).cast(IntegerType()).alias('golf_handicap'),
            lit(None).cast(StringType()).alias('dma_region'),
            lit(None).cast(StringType()).alias('course_home'),
            lit(None).cast(BooleanType()).alias('has_coach'),
            lit(None).cast(StringType()).alias('volunteer_history'),
            lit(None).cast(TimestampType()).alias('sign_up_at'),
            lit(None).cast(StringType()).alias('sign_up_source'),
            lit(None).cast(DateType()).alias('birth_date'),
            col('gender').cast(StringType()).alias('gender'),
            col('juniorParent').cast(BooleanType()).alias('parent_of_junior'),
            lit(None).cast(StringType()).alias('league'),
            lit(None).cast(StringType()).alias('team'),
            col('address1'),
            col('address2'),
            col('city'),
            col('state'),
            lit(None).alias('country'),
            col('zip').alias('postal_code'))

cc_consumers = ccms.select(col('cust_id').cast(StringType()).alias('source_consumer_id'),
            col('source_system'),
            col('first_nm').cast(StringType()).alias('name_first'),
            col('last_nm').cast(StringType()).alias('name_last'),
            col('cyber_txt').cast(StringType()).alias('email'),
            lit(None).cast(BooleanType()).alias('do_not_sell'),
            lit(None).cast(DoubleType()).alias('total_ticket_spend'),
            lit(None).cast(DoubleType()).alias('total_merch_spend'),
            lit(None).cast(DoubleType()).alias('total_pga_spend'),
            lit(None).cast(BooleanType()).alias('secondary_purchaser'),
            lit(None).cast(BooleanType()).alias('secondary_seller'),
            lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
            lit(None).cast(BooleanType()).alias('travel_purchaser'),
            lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
            lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
            lit(None).cast(BooleanType()).alias('internal'),
            lit(None).cast(BooleanType()).alias('vip_status'),
            lit(None).cast(StringType()).alias('pga_professional_id'),
            lit(None).cast(IntegerType()).alias('golf_handicap'),
            lit(None).cast(StringType()).alias('dma_region'),
            lit(None).cast(StringType()).alias('course_home'),
            lit(None).cast(BooleanType()).alias('has_coach'),
            lit(None).cast(StringType()).alias('volunteer_history'),
            lit(None).cast(TimestampType()).alias('sign_up_at'),
            lit(None).cast(StringType()).alias('sign_up_source'),
            lit(None).cast(DateType()).alias('birth_date'),
            lit(None).cast(StringType()).alias('gender'),
            lit(None).cast(BooleanType()).alias('parent_of_junior'),
            lit(None).cast(StringType()).alias('league'),
            lit(None).cast(StringType()).alias('team'),
            col('street1'),
            col('street2'),
            col('city_nm'),
            col('state_cd'),
            col('country_nm'),
            col('postal_cd').alias('postal_code'))

i_consumers = iterable_api.select(lit(None).cast(StringType()).alias('source_consumer_id'),
            col('source_system'),
            col('name_first').cast(StringType()),
            col('name_last').cast(StringType()),
            col('email').cast(StringType()),
            lit(None).cast(BooleanType()).alias('do_not_sell'),
            lit('total_ticket_spend').cast(DoubleType()),
            lit(None).cast(DoubleType()).alias('total_merch_spend'),
            lit(None).cast(DoubleType()).alias('total_pga_spend'),
            lit(None).cast(BooleanType()).alias('secondary_purchaser'),
            lit(None).cast(BooleanType()).alias('secondary_seller'),
            lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
            lit(None).cast(BooleanType()).alias('travel_purchaser'),
            lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
            lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
            lit(None).cast(BooleanType()).alias('internal'),
            lit(None).cast(BooleanType()).alias('vip_status'),
            lit(None).cast(StringType()).alias('pga_professional_id'),
            lit(None).cast(IntegerType()).alias('golf_handicap'),
            lit(None).cast(StringType()).alias('dma_region'),
            lit(None).cast(StringType()).alias('course_home'),
            lit(None).cast(BooleanType()).alias('has_coach'),
            lit(None).cast(StringType()).alias('volunteer_history'),
            lit(None).cast(TimestampType()).alias('sign_up_at'),
            lit(None).cast(StringType()).alias('sign_up_source'),
            lit(None).cast(DateType()).alias('birth_date'),
            lit(None).cast(StringType()).alias('gender'),
            lit('parent_of_junior').cast(BooleanType()),
            lit(None).cast(StringType()).alias('league'),
            lit(None).cast(StringType()).alias('team'),
            col('address_1'),
            col('address_2'),
            col('city'),
            col('state_region'),
            col('country'),
            col('postal_code'))

s_consumers = shopify.select(col('customer_id').cast(StringType()).alias('source_customer_id'),
            col('source_system'),
            col('first_name'),
            col('last_name'),
            col('email'),
            lit(None).cast(BooleanType()).alias('do_not_sell'),
            lit('total_ticket_spend').cast(DoubleType()),
            lit(None).cast(DoubleType()).alias('total_merch_spend'),
            lit(None).cast(DoubleType()).alias('total_pga_spend'),
            lit(None).cast(BooleanType()).alias('secondary_purchaser'),
            lit(None).cast(BooleanType()).alias('secondary_seller'),
            lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
            lit(None).cast(BooleanType()).alias('travel_purchaser'),
            lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
            lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
            lit(None).cast(BooleanType()).alias('internal'),
            lit(None).cast(BooleanType()).alias('vip_status'),
            lit(None).cast(StringType()).alias('pga_professional_id'),
            lit(None).cast(IntegerType()).alias('golf_handicap'),
            lit(None).cast(StringType()).alias('dma_region'),
            lit(None).cast(StringType()).alias('course_home'),
            lit(None).cast(BooleanType()).alias('has_coach'),
            lit(None).cast(StringType()).alias('volunteer_history'),
            lit(None).cast(TimestampType()).alias('sign_up_at'),
            lit(None).cast(StringType()).alias('sign_up_source'),
            lit(None).cast(DateType()).alias('birth_date'),
            lit(None).cast(StringType()).alias('gender'),
            lit(None).cast(BooleanType()).alias('parent_of_junior'),
            lit(None).cast(StringType()).alias('league'),
            lit(None).cast(StringType()).alias('team'),
            col('address1'),
            col('address2'),
            col('city'),
            col('state_region'),
            col('country'),
            col('zip').alias('postal_code'))

tr_consumers = ticket_registry.select(lit(None).cast(StringType()).alias('source_consumer_id'),
            col('source_system'),
            col('name_first'),
            col('name_last'),
            col('email'),
            lit(None).cast(BooleanType()).alias('do_not_sell'),
            lit(None).cast(DoubleType()).alias('total_ticket_spend'),
            lit(None).cast(DoubleType()).alias('total_merch_spend'),
            lit(None).cast(DoubleType()).alias('total_pga_spend'),
            lit(None).cast(BooleanType()).alias('secondary_purchaser'),
            lit(None).cast(BooleanType()).alias('secondary_seller'),
            lit(None).cast(BooleanType()).alias('trade_in_purchaser'),
            lit(None).cast(BooleanType()).alias('travel_purchaser'),
            lit(None).cast(DoubleType()).alias('merch_purchase_likely'),
            lit(None).cast(DoubleType()).alias('ticket_purchase_likely'),
            lit(None).cast(BooleanType()).alias('internal'),
            lit(None).cast(BooleanType()).alias('vip_status'),
            lit(None).cast(StringType()).alias('pga_professional_id'),
            lit(None).cast(IntegerType()).alias('golf_handicap'),
            lit(None).cast(StringType()).alias('dma_region'),
            lit(None).cast(StringType()).alias('course_home'),
            lit(None).cast(BooleanType()).alias('has_coach'),
            lit(None).cast(StringType()).alias('volunteer_history'),
            lit(None).cast(TimestampType()).alias('sign_up_at'),
            lit(None).cast(StringType()).alias('sign_up_source'),
            lit(None).cast(DateType()).alias('birth_date'),
            lit(None).cast(StringType()).alias('gender'),
            lit(None).cast(BooleanType()).alias('parent_of_junior'),
            lit(None).cast(StringType()).alias('league'),
            lit(None).cast(StringType()).alias('team'),
            lit(None).cast(StringType()).alias('address_1'),
            lit(None).cast(StringType()).alias('address_2'),
            lit(None).cast(StringType()).alias('city'),
            lit(None).cast(StringType()).alias('state_region'),
            lit(None).cast(StringType()).alias('country'),
            col('postal_code'))

consumers_temp_stg = r_consumers.union(sm_consumers).union(a_consumers).union(sfcs_consumers).union(sfc_consumers).union(sfr_consumers).union(ct_consumers).union(cc_consumers).union(i_consumers).union(s_consumers).union(tr_consumers)
consumers_temp_stg.cache()
consumers_temp_stg.count()

consumers_temp_stg.createOrReplaceTempView("cts")
id_temp_stg = spark.sql("select *, uuid() as id from (select distinct name_first, name_last, email from cts where email is not null)")
id_temp_stg.cache()
id_temp_stg.count()

id_temp_stg.createOrReplaceTempView("idt")
consumers_id_temp_stg = spark.sql("select idt.id, cts.email, cts.source_system, cts.name_first, cts.name_last, cts.birth_date, cts.gender, cts.parent_of_junior, cts.league, cts.team, cts.do_not_sell, cts.total_ticket_spend, cts.total_merch_spend, cts.total_pga_spend, cts.secondary_purchaser, cts.secondary_seller, cts.trade_in_purchaser, cts.travel_purchaser, cts.merch_purchase_likely, cts.ticket_purchase_likely, cts.internal, cts.vip_status, cts.pga_professional_id, cts.golf_handicap, cts.dma_region, cts.course_home, cts.has_coach, cts.volunteer_history, cts.sign_up_at, cts.sign_up_source, cts.address_1, cts.address_2, cts.city, cts.state, cts.country, cts.postal_code from cts join idt on cts.name_first <=> idt.name_first and cts.name_last <=> idt.name_last and cts.email == idt.email")
consumers_id_temp_stg = consumers_id_temp_stg.dropDuplicates(['id','address_1','address_2','city','country','postal_code'])
consumers_stg = consumers_id_temp_stg.dropDuplicates(['id']).drop('address_1','address_2','city','state','country','postal_code','source_system')
consumers_id_temp_stg.cache()
consumers_stg.cache()
consumers_stg.count()
table_dict['consumers'] = consumers_stg

r_address = roster.select(
             col('address_1').cast(StringType()).alias('address_1'),
             col('address_2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country').cast(StringType()).alias('country'),
             col('zip_postal_code').cast(StringType()).alias('postal_code'),
             lit(None).cast(StringType()).alias('latitude'),
             lit(None).cast(StringType()).alias('longitude'))

a_address_a = axs.select(
             col('address_1').cast(StringType()).alias('address_1'),
             col('address_2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state_region').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country').cast(StringType()).alias('country'),
             col('postal_code').cast(StringType()).alias('postal_code'),
             lit(None).cast(StringType()).alias('latitude'),
             lit(None).cast(StringType()).alias('longitude'))

a_address_b = axs.select(
             col('billing_address_1').cast(StringType()).alias('address_1'),
             col('billing_address_2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('billing_city').cast(StringType()).alias('city'),
             col('billing_state_region').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('billing_country').cast(StringType()).alias('country'),
             col('billing_postal_code').cast(StringType()).alias('postal_code'),
             lit(None).cast(StringType()).alias('latitude'),
             lit(None).cast(StringType()).alias('longitude'))

a_address = a_address_a.union(a_address_b)

sfcs_address = sf_career_services.select(
             col('Address1').cast(StringType()).alias('address_1'),
             col('Address2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('MailingCity').cast(StringType()).alias('city'),
             col('MailingState').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('MailingCountry').cast(StringType()).alias('country'),
             col('MailingPostalCode').cast(StringType()).alias('postal_code'),
             lit(None).cast(StringType()).alias('latitude'),
             lit(None).cast(StringType()).alias('longitude'))

sfc_address = sf_champs.select(
            col('Address1').cast(StringType()).alias('address_1'),
            col('Address2').cast(StringType()).alias('address_2'),
            lit(None).cast(StringType()).alias('address_3'),
            lit(None).cast(StringType()).alias('address_4'),
            col('city'),
            col('state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            col('country'),
            col('postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude')) 

sfr_address = sf_reach.select(
            col('Address1').cast(StringType()).alias('address_1'),
            col('Address2').cast(StringType()).alias('address_2'),
            lit(None).cast(StringType()).alias('address_3'),
            lit(None).cast(StringType()).alias('address_4'),
            col('city'),
            col('state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            col('country'),
            col('postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))          

ct_address = coach.select(
            col('address1').cast(StringType()).alias('address_1'),
            col('address2').cast(StringType()).alias('address_2'),
            lit(None).cast(StringType()).alias('address_3'),
            lit(None).cast(StringType()).alias('address_4'),
            col('city').cast(StringType()).alias('city'),
            col('state').cast(StringType()).alias('state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            lit(None).cast(StringType()).alias('country'),
            col('zip').cast(StringType()).alias('postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))

cc_address = ccms.select(
            col('street1').cast(StringType()).alias('address_1'),
            col('street2').cast(StringType()).alias('address_2'),
            col('street3').cast(StringType()).alias('address_3'),
            col('street4').cast(StringType()).alias('address_4'),
            col('city_nm').cast(StringType()).alias('city'),
            col('state_cd').cast(StringType()).alias('state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            col('country_nm').cast(StringType()).alias('country'),
            col('postal_cd').cast(StringType()).alias('postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))

i_address = iterable_api.select(
            col('address_1').cast(StringType()),
            col('address_2').cast(StringType()),
            lit(None).cast(StringType()).alias('address_3'),
            lit(None).cast(StringType()).alias('address_4'),
            col('city').cast(StringType()),
            col('state_region').cast(StringType()),
            lit(None).cast(StringType()).alias('int_region'),
            col('country').cast(StringType()),
            col('postal_code').cast(StringType()),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))

s_address_a = shopify.select(
            col('address1').cast(StringType()).alias('address_1'),
            col('address2').cast(StringType()).alias('address_2'),
            lit(None).cast(StringType()).alias('address_3'),
            lit(None).cast(StringType()).alias('address_4'),
            col('city'),
            col('state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            col('country'),
            col('zip').cast(StringType()).alias('postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))

s_address_b = shopify.select(
            col('billing_address_1'),
            col('billing_address_2'),
            lit(None).cast(StringType()).alias('billing_address_3'),
            lit(None).cast(StringType()).alias('billing_address_4'),
            col('billing_city'),
            col('billing_state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            col('billing_country'),
            col('billing_postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))

s_address_c = shopify.select(
            col('shipping_address_1'),
            col('shipping_address_2'),
            lit(None).cast(StringType()).alias('shipping_address_3'),
            lit(None).cast(StringType()).alias('shipping_address_4'),
            col('shipping_city'),
            col('shipping_state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            col('shipping_country'),
            col('shipping_postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))

s_address = s_address_a.union(s_address_b).union(s_address_c)

tr_address = ticket_registry.select(
            lit(None).cast(StringType()).alias('address_1'),
            lit(None).cast(StringType()).alias('address_2'),
            lit(None).cast(StringType()).alias('address_3'),
            lit(None).cast(StringType()).alias('address_4'),
            lit(None).cast(StringType()).alias('city'),
            lit(None).cast(StringType()).alias('state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            lit(None).cast(StringType()).alias('country'),
            col('postal_code').cast(StringType()),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))

address_temp_stg = r_address.union(a_address).union(sfcs_address).union(sfc_address).union(sfr_address).union(ct_address).union(cc_address).union(i_address).union(s_address).union(tr_address)
address_temp_stg.count()

address_temp_stg = address_temp_stg.dropDuplicates()
address_temp_stg = address_temp_stg.replace("null",None)
address_temp_stg = address_temp_stg.dropna(how="all")
address_stg = address_temp_stg.withColumn('id',expr("uuid()"))
address_stg.cache()
address_stg.count()

address_stg.createOrReplaceTempView("address")
consumer_address_temp_stg = spark.sql("select id as address_id, address_1, address_2, city, state_region, country, postal_code from address")
consumer_address_temp_stg.createOrReplaceTempView('cats')
consumers_id_temp_stg.createOrReplaceTempView('cits')
consumer_address_stg = spark.sql("select *, uuid() as id from (select distinct cits.id as consumer_id, cats.address_id from cits join cats on cits.address_1 <=> cats.address_1 and cits.address_2 <=> cats.address_2 and cits.city <=> cats.city and cits.state <=> cats.state_region and cits.country <=> cats.country and cits.postal_code <=> cats.postal_code)")
consumer_address_stg.cache()

table_dict['consumer_addresses'] = consumer_address_stg

sm_venue_addresses = splash_main.select(
            col('address').cast(StringType()).alias('address_1'),
            lit(None).cast(StringType()).alias('address_2'),
            lit(None).cast(StringType()).alias('address_3'),
            lit(None).cast(StringType()).alias('address_4'),
            col('city').cast(StringType()).alias('city'),
            col('state').cast(StringType()).alias('state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            col('country').cast(StringType()).alias('country'),
            col('zip_code').cast(StringType()).alias('postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))

axs_venue_addresses = axs.select(
            col('venue_address_1').cast(StringType()).alias('address_1'),
            col('venue_address_2').cast(StringType()).alias('address_2'),
            lit(None).cast(StringType()).alias('address_3'),
            lit(None).cast(StringType()).alias('address_4'),
            col('venue_city').cast(StringType()).alias('city'),
            col('venue_state_region').cast(StringType()).alias('state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            col('venue_country').cast(StringType()).alias('venue_country'),
            col('venue_postal_code').cast(StringType()).alias('postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))

sfc_venue_addresses = sf_champs.select(col('venue_address_1'),
            lit(None).cast(StringType()).alias('venue_address_2'),
            lit(None).cast(StringType()).alias('address_3'),
            lit(None).cast(StringType()).alias('address_4'),
            col('venue_city'),
            col('venue_state_region'),
            lit(None).cast(StringType()).alias('int_region'),
            col('venue_country').alias('country'),
            col('venue_postal_code').alias('postal_code'),
            lit(None).cast(StringType()).alias('latitude'),
            lit(None).cast(StringType()).alias('longitude'))
venue_address_temp_stg = sm_venue_addresses.union(axs_venue_addresses).union(sfc_venue_addresses).dropDuplicates()

venue_address_temp_stg = venue_address_temp_stg.dropna(how="all")
venue_address_temp_stg = venue_address_temp_stg.withColumn("id",expr("uuid()"))
address_stg = address_stg.union(venue_address_temp_stg)
table_dict['addresses'] = address_stg

axs_events = axs.select(col('name_first').cast(StringType()).alias('name_first'),
                      col('name_last').cast(StringType()).alias('name_last'),
                      col('email').cast(StringType()).alias('email'),
                      col('venue_address_1').cast(StringType()).alias('address'),
                      col('venue_city').cast(StringType()).alias('city'),
                      col('venue_state_region').cast(StringType()).alias('state'),
                      col('venue_postal_code').cast(StringType()).alias('zip_code'),
                      col('venue_country').cast(StringType()).alias('country'),
                      lit(None).cast(StringType()).alias('event_id'),
                      lit(None).cast(TimestampType()).alias('date_rsvped'),
                      lit(None).cast(BooleanType()).alias('attending'),
                      lit(None).cast(BooleanType()).alias('checked_in'),
                      lit(None).cast(BooleanType()).alias('vip'),
                      lit(None).cast(BooleanType()).alias('has_waitlist'),
                      col('event_unique_id').cast(StringType()).alias('source_event_id'),
                      col('event_name'),
                      lit(None).cast(StringType()).alias('event_description'),
                      col('venue_name').cast(StringType()).alias('venue_name'),
                      col('event_datetime').cast(TimestampType()).alias('start_time'),
                      lit(None).cast(TimestampType()).alias('end_time'),
                      lit(None).cast(BooleanType()).alias('rsvp_open'),
                      lit(None).cast(BooleanType()).alias('on_waitlist'),
                      lit(None).cast(TimestampType()).alias('rsvp_closed_at'))

sm_events = splash_main.select(col('first_name').cast(StringType()).alias('name_first'),
                      col('last_name').cast(StringType()).alias('name_last'),
                      col('primary_email').cast(StringType()).alias('email'),
                      col('address').cast(StringType()),
                      col('city'),
                      col('state'),
                      col('zip_code'),
                      col('country'),
                      col('event_id').cast(StringType()).alias('event_id'),
                      col('date_rsvped').cast(TimestampType()).alias('date_rsvped'),
                      col('attending').cast(BooleanType()),
                      col('checked_in').cast(BooleanType()).alias('checked_in'),
                      col('vip').cast(BooleanType()).alias('vip'),
                      col('event_waitlist').cast(BooleanType()).alias('has_waitlist'),
                      col('source_event_id').cast(StringType()),
                      col('event_name'),
                      col('event_description'),
                      col('venue_name'),
                      col('start_time').cast(TimestampType()),
                      col('end_time').cast(TimestampType()),
                      lit('rsvp_open').cast(BooleanType()),
                      col('consumer_waitlist').cast(BooleanType()).alias('on_waitlist'),
                      col('rsvp_closed_at').cast(TimestampType()))

sfc_events = sf_champs.select(col('name_first'),
                 col('name_last'),
                 col('email'),
                 col('venue_address_1').cast(StringType()).alias('address'),
                 col('venue_city').cast(StringType()).alias('city'),
                 col('venue_state_region').cast(StringType()).alias('state'),
                 col('venue_postal_code').cast(StringType()).alias('zip_code'),
                 col('venue_country').cast(StringType()).alias('country'),
                 lit(None).cast(StringType()).alias('event_id'),
                 lit(None).cast(TimestampType()).alias('date_rsvped'),
                 lit(None).cast(BooleanType()).alias('attending'),
                 lit(None).cast(BooleanType()).alias('checked_in'),
                 lit(None).cast(BooleanType()).alias('vip'),
                 lit(None).cast(BooleanType()).alias('has_waitlist'),
                 lit(None).cast(StringType()).alias('source_event_id'),
                 col('event_name'),
                 lit(None).cast(StringType()).alias('event_description'),
                 col('venue_name'),
                 col('start_time').cast(TimestampType()),
                 col('end_time').cast(TimestampType()),
                 lit(None).cast(BooleanType()).alias('rsvp_open'),
                 lit(None).cast(BooleanType()).alias('on_waitlist'),
                 lit(None).cast(TimestampType()).alias('rsvp_closed_at'))

events_master_temp_stg = axs_events.union(sm_events).union(sfc_events)
events_master_temp_stg = events_master_temp_stg.withColumn('year',substring('start_time',0,4))
events_master_temp_stg.createOrReplaceTempView("emts")

events_temp_id_stg = spark.sql("select *, uuid() as event_id from (select distinct event_name, venue_name, year from emts where event_name is not null)")
events_temp_id_stg.createOrReplaceTempView('etis')
events_master_stg = spark.sql("select etis.event_id as id, emts.name_first, emts.name_last, emts.email, emts.address, emts.city, emts.state, emts.zip_code, emts.country, emts.date_rsvped, emts.attending, emts.checked_in, emts.vip, emts.on_waitlist, emts.source_event_id, emts.event_name, emts.event_description, emts.venue_name, emts.start_time, emts.end_time, emts.rsvp_open, emts.has_waitlist, emts.rsvp_closed_at from etis join emts on etis.event_name = emts.event_name and etis.venue_name <=> emts.venue_name and etis.year <=> emts.year")

events_master_stg.createOrReplaceTempView("ems")
events_temp_stg = spark.sql("select id, source_event_id, event_name, event_description, venue_name, start_time, end_time, rsvp_open, has_waitlist, rsvp_closed_at, address, city, state, zip_code, country from ems")
events_temp_stg.createOrReplaceTempView('ets')
address_stg.createOrReplaceTempView('address')
events_address_stg = spark.sql("select ets.id as id, ets.source_event_id, ets.event_name, ets.event_description, ets.venue_name, a.id as address_id, ets.start_time, ets.end_time, ets.rsvp_open, ets.has_waitlist, ets.rsvp_closed_at from ets left join address a on ets.address = a.address_1 and ets.city = a.city and ets.state = a.state_region and ets.zip_code = a.postal_code and ets.country = a.country")
events_stg = events_address_stg.dropDuplicates(['id'])
table_dict['events'] = events_stg
consumer_events_temp_stg = spark.sql("select id, date_rsvped, attending, checked_in, vip, on_waitlist, name_first, name_last, email from ems")
consumer_events_temp_stg.createOrReplaceTempView('cets')
consumers_stg.createOrReplaceTempView('cs')
consumer_events_stg = spark.sql("select *, uuid() as id from (select distinct cets.id as event_id, cs.id as consumer_id, cets.date_rsvped, cets.attending, cets.checked_in, cets.vip, cets.on_waitlist from cets join cs on cets.name_first <=> cs.name_first and cets.name_last <=> cs.name_last and cets.email == cs.email)")
table_dict['consumer_events'] = consumer_events_stg
roster_source_system = roster.select(lit(None).cast(StringType()).alias('name'),
                                lit(None).cast(StringType()).alias('url'),
                                col('source_system').alias('platform'))
sm_source_system = splash_main.select(lit(None).cast(StringType()).alias('name'),
                                      lit(None).cast(StringType()).alias('url'),
                                      col('source_system').alias('platform'))
axs_source_system = axs.select(lit(None).cast(StringType()).alias('name'),
                                lit(None).cast(StringType()).alias('url'),
                                col('source_system').alias('platform'))
sfcs_source_system = sf_career_services.select(lit(None).cast(StringType()).alias('name'),
                                lit(None).cast(StringType()).alias('url'),
                                col('source_system').alias('platform'))
sfc_source_system = sf_champs.select(lit(None).cast(StringType()).alias('name'),
                                lit(None).cast(StringType()).alias('url'),
                                col('source_system').alias('platform'))
sfr_source_system = sf_reach.select(lit(None).cast(StringType()).alias('name'),
                                    lit(None).cast(StringType()).alias('url'),
                                    col('source_system').alias('platform'))
ct_source_system = coach.select(lit(None).cast(StringType()).alias('name'),
                                lit(None).cast(StringType()).alias('url'),
                                col('source_system').alias('platform'))
cc_source_system = ccms.select(lit(None).cast(StringType()).alias('name'),
                                lit(None).cast(StringType()).alias('url'),
                                col('source_system').alias('platform'))
i_source_system = iterable_api.select(lit(None).cast(StringType()).alias('name'),
                                lit(None).cast(StringType()).alias('url'),
                                col('source_system').alias('platform'))
s_source_system = shopify.select(col('source_instance').cast(StringType()).alias('name'),
                                 lit(None).cast(StringType()).alias('url'),
                                 col('source_system').alias('platform'))
tr_source_system = ticket_registry.select(lit(None).cast(StringType()).alias('name'),
                                          lit(None).cast(StringType()).alias('url'),
                                          col('source_system').alias('platform'))

source_system_temp_stg = roster_source_system.union(axs_source_system).union(sm_source_system).union(sfcs_source_system).union(sfc_source_system).union(sfr_source_system).union(ct_source_system).union(cc_source_system).union(i_source_system).union(s_source_system).union(tr_source_system).distinct()
source_system_temp_stg.createOrReplaceTempView('ssts')
source_system_stg = spark.sql("select row_number() over (order by platform) as id, name, url, platform from ssts")
table_dict['source_system'] = source_system_stg

axs_orders = axs.select(
                      col('name_first'),
                      col('name_last'),
                      col('email'),
                      col('source_system'),
                      lit(None).cast(StringType()).alias('source_instance'),
                      col('customer_unique_id').cast(StringType()).alias('source_consumer_id'),
                      col('source_order_id'),
                      col('order_date').cast(TimestampType()).alias('order_date'),
                      lit(None).cast(StringType()).alias('campaign'),
                      lit(None).cast(StringType()).alias('site'),
                      lit(None).cast(DoubleType()).alias('total_discount'),
                      lit(None).cast(DoubleType()).alias('total_gross_sub'),
                      lit(None).cast(DoubleType()).alias('total_taxable_sub'),
                      lit(None).cast(DoubleType()).alias('total_non_taxable_sub'),
                      lit(None).cast(DoubleType()).alias('total_tax'),
                      lit(None).cast(DoubleType()).alias('total_ship'),
                      col('total').cast(DoubleType()).alias('total'),
                      col('source_product_id').cast(StringType()).alias('source_product_id'),
                      lit(None).cast(StringType()).alias('product_name'),
                      lit(None).cast(DoubleType()).alias('quantity'),
                      col('unit_price').cast(DoubleType()).alias('unit_price'),
                      lit(None).cast(DoubleType()).alias('item_discount'),
                      lit(None).cast(StringType()).alias('category'),
                      lit(None).cast(StringType()).alias('sub_category'),
                      lit(None).cast(StringType()).alias('sub_class'),
                      lit(None).cast(StringType()).alias('variation_size'),
                      col('event_name'),
                      col('event_datetime'),
                      col('venue_name'))

sfc_orders = sf_champs.select(col('name_first'),
                              col('name_last'),
                              col('email'),
                              col('source_system'),
                              lit(None).cast(StringType()).alias('source_instance'),
                              col('source_consumer_id'),
                              col('source_order_id'),
                              col('order_date').cast(TimestampType()).alias('order_date'),
                              lit(None).cast(StringType()).alias('campaign'),
                              lit(None).cast(StringType()).alias('site'),
                              lit(None).cast(DoubleType()).alias('total_discount'),
                              lit(None).cast(DoubleType()).alias('total_gross_sub'),
                              lit(None).cast(DoubleType()).alias('total_taxable_sub'),
                              lit(None).cast(DoubleType()).alias('total_non_taxable_sub'),
                              lit(None).cast(DoubleType()).alias('total_tax'),
                              lit(None).cast(DoubleType()).alias('total_ship'),
                              col('total').cast(DoubleType()).alias('total'),
                              col('source_product_id'),
                              col('product_name'),
                              col('quantity').cast(DoubleType()),
                              col('unit_price').cast(DoubleType()),
                              lit(None).cast(DoubleType()).alias('item_discount'),
                              lit(None).cast(StringType()).alias('category'),
                              lit(None).cast(StringType()).alias('sub_category'),
                              lit(None).cast(StringType()).alias('sub_class'),
                              lit(None).cast(StringType()).alias('variation_size'),
                              col('event_name'),
                              col('start_time'),
                              col('venue_name'))

sfr_orders = sf_reach.select(col('name_first'),
                             col('name_last'),
                             col('email'),
                             col('source_system'),
                             lit(None).cast(StringType()).alias('source_instance'),
                             col('id').alias('source_consumer_id'),
                             col('source_order_id'),
                             col('order_date'),
                             lit(None).cast(StringType()).alias('campaign'),
                             lit(None).cast(StringType()).alias('site'),
                             lit(None).cast(DoubleType()).alias('total_discount'),
                             lit(None).cast(DoubleType()).alias('total_gross_sub'),
                             lit(None).cast(DoubleType()).alias('total_taxable_sub'),
                             lit(None).cast(DoubleType()).alias('total_non_taxable_sub'),
                             lit(None).cast(DoubleType()).alias('total_tax'),
                             lit(None).cast(DoubleType()).alias('total_ship'),
                             col('total'),
                             lit(None).cast(StringType()).alias('source_product_id'),
                             lit(None).cast(StringType()).alias('product_name'),
                             lit(None).cast(DoubleType()).alias('quantity'),
                             lit(None).cast(DoubleType()).alias('unit_price'),
                             lit(None).cast(DoubleType()).alias('item_discount'),
                             lit(None).cast(StringType()).alias('category'),
                             lit(None).cast(StringType()).alias('sub_category'),
                             lit(None).cast(StringType()).alias('sub_class'),
                             lit(None).cast(StringType()).alias('variation_size'),
                             lit(None).cast(StringType()).alias('event_name'),
                             lit(None).cast(StringType()).alias('start_time'),
                             lit(None).cast(StringType()).alias('venue_name'))

s_orders = shopify.select(col('first_name'),
                          col('last_name'),
                          col('email'),
                          col('source_system'),
                          col('source_instance'),
                          col('customer_id').alias('source_consumer_id'),
                          col('source_order_id'),
                          col('order_date').cast(TimestampType()).alias('order_date'),
                          lit(None).cast(StringType()).alias('campaign'),
                          lit(None).cast(StringType()).alias('site'),
                          col('total_discounts').cast(DoubleType()),
                          col('total_gross_sub').cast(DoubleType()),
                          lit(None).cast(DoubleType()).alias('total_taxable_sub'),
                          lit(None).cast(DoubleType()).alias('total_non_taxable_sub'),
                          col('total_tax').cast(DoubleType()),
                          col('total_ship').cast(DoubleType()),
                          col('total_price').cast(DoubleType()).alias('total'),
                          col('product_id').cast(StringType()).alias('source_product_id'),
                          col('title').cast(StringType()).alias('product_name'),
                          col('quantity').cast(DoubleType()),
                          col('price').cast(DoubleType()).alias('unit_price'),
                          col('total_discount').cast(DoubleType()).alias('item_discount'),
                          lit(None).cast(StringType()).alias('category'),
                          lit(None).cast(StringType()).alias('sub_category'),
                          lit(None).cast(StringType()).alias('sub_class'),
                          lit(None).cast(StringType()).alias('variation_size'),
                          lit(None).cast(StringType()).alias('event_name'),
                          lit(None).cast(StringType()).alias('start_time'),
                          lit(None).cast(StringType()).alias('venue_name'))

orders_temp_master_stg = axs_orders.union(sfc_orders).union(sfr_orders).union(s_orders)

a_shipping_address = axs.select(
                      col('source_order_id'),
                      col('source_system'),
                      lit(None).cast(StringType()).alias('source_instance'),
                      col('address_1').alias('shipping_address_1'),
                      col('address_2').alias('shipping_address_2'),
                      col('city').alias('shipping_city'),
                      col('state_region').alias('shipping_state'),
                      col('postal_code').alias('shipping_postal_code'),
                      col('country').alias('shipping_country'))


a_billing_address = axs.select(
                      col('source_order_id'),
                      col('source_system'),
                      lit(None).cast(StringType()).alias('source_instance'),
                      col('billing_address_1'),
                      col('billing_address_2'),
                      col('billing_city'),
                      col('billing_state_region'),
                      col('billing_postal_code'),
                      col('billing_country'))

s_shipping_address = shopify.select(
                        col('source_order_id'),
                        col('source_system'),
                        col('source_instance'),
                        col('shipping_address_1'),
                        col('shipping_address_2'),
                        col('shipping_city'),
                        col('shipping_state_region'),
                        col('shipping_postal_code'),
                        col('shipping_country'))

s_billing_address = shopify.select(
                        col('source_order_id'),
                        col('source_system'),
                        col('source_instance'),
                        col('billing_address_1'),
                        col('billing_address_2'),
                        col('billing_city'),
                        col('billing_state_region'),
                        col('billing_postal_code'),
                        col('billing_country'))

orders_shipping_address = a_shipping_address.union(s_shipping_address)
orders_billing_address = a_billing_address.union(s_billing_address)
orders_temp_master_stg.cache()
orders_temp_master_stg.count()

orders_shipping_address.createOrReplaceTempView("ship")
orders_billing_address.createOrReplaceTempView("bill")
orders_temp_master_stg.createOrReplaceTempView("otms")

orders_shipping_address_id = spark.sql("select ship.source_order_id, ship.source_system, ship.source_instance, a.id as address_shipping_id from ship left join address a on ship.shipping_address_1 = a.address_1 and ship.shipping_address_2 = a.address_2 and ship.shipping_city = a.city and ship.shipping_state = a.state_region and ship.shipping_country = a.country")
orders_billing_address_id = spark.sql("select bill.source_order_id, bill.source_system, bill.source_instance, a.id as address_billing_id from bill left join address a on bill.billing_address_1 = a.address_1 and bill.billing_address_2 = a.address_2 and bill.billing_city = a.city and bill.billing_state_region = a.state_region and bill.billing_country = a.country")

orders_shipping_address_id.createOrReplaceTempView("osai")
orders_billing_address_id.createOrReplaceTempView("obai")
source_system_stg.createOrReplaceTempView("sss")
consumers_stg.createOrReplaceTempView("cs")

orders_temp_stg = spark.sql("select uuid() as id, cs.id as consumer_id, sss.id as source_system_id, otms.source_system, otms.source_instance, otms.source_order_id, osai.address_shipping_id, obai.address_billing_id, otms.order_date, otms.campaign, otms.site, otms.total_discount, otms.total_gross_sub, otms.total_taxable_sub, otms.total_non_taxable_sub, otms.total_tax, otms.total_ship, otms.total from otms join cs on otms.name_first <=> cs.name_first and otms.name_last <=> cs.name_last and otms.email == cs.email left join osai on (otms.source_order_id = osai.source_order_id and otms.source_system = osai.source_system and otms.source_instance <=> osai.source_instance) left join obai on (otms.source_order_id = obai.source_order_id and otms.source_system = obai.source_system and otms.source_instance <=> obai.source_instance) left join sss on (otms.source_system = sss.platform and otms.source_instance <=> sss.name) where otms.source_order_id is not null")
orders_temp_stg = orders_temp_stg.dropna(how="all",subset=['source_system_id','order_date', 'campaign', 'site', 'total_discount', 'total_gross_sub', 'total_taxable_sub', 'total_non_taxable_sub', 'total_tax', 'total_ship', 'total'])
orders_stg = orders_temp_stg.dropDuplicates(['source_order_id','source_system', 'source_instance']).drop('source_system', 'source_instance')
table_dict["orders"] = orders_stg

order_items_temp_stg = spark.sql("select distinct source_order_id, source_system, source_instance, source_product_id, product_name, quantity, unit_price, item_discount, category, sub_category, sub_class, variation_size, event_name, substring(event_datetime,0,4) as year, venue_name from otms where source_order_id is not null or source_product_id is not null")
order_items_temp_stg = order_items_temp_stg.dropna(how='all',subset=['source_order_id','source_product_id','product_name','quantity','unit_price','item_discount','event_name'])

order_items_temp_stg.createOrReplaceTempView("oits")
orders_temp_stg.createOrReplaceTempView("ots")
order_items_orders_temp_stg = spark.sql("select ots.id as order_id, oits.source_product_id, oits.product_name, oits.quantity, oits.unit_price, oits.item_discount, oits.category, oits.sub_category, oits.sub_class, oits.variation_size, oits.event_name, oits.year, oits.venue_name from ots left join oits on oits.source_order_id <=> ots.source_order_id and oits.source_system == ots.source_system and oits.source_instance <=> ots.source_instance")

order_items_orders_temp_stg.createOrReplaceTempView("oiots")
events_stg.createOrReplaceTempView("es")
order_items_events_temp_stg = spark.sql("select *, uuid() as id from (select oiots.order_id, oiots.source_product_id, es.id as event_id, oiots.product_name, oiots.quantity, oiots.unit_price, oiots.item_discount, oiots.category, oiots.sub_category, oiots.sub_class, oiots.variation_size from oiots left join es on oiots.event_name = es.event_name and oiots.year <=> substring(es.start_time,0,4) and oiots.venue_name <=> es.venue_name)")
order_items_stg = order_items_events_temp_stg.dropna(how="all",subset=["product_name","quantity","unit_price","item_discount","category","sub_category","sub_class","variation_size"])
table_dict['order_items'] = order_items_stg

marketing_master_stg = iterable_api
marketing_master_stg.createOrReplaceTempView("mms")
marketing_campaigns_temp_stg = spark.sql("select uuid() as id, source_campaign_id, campaign_name, cast(template_id as int), message_medium, created_by_user_id, updated_by_user_id, campaign_state, campaign_type, created_at from mms where source_campaign_id is not null")
marketing_campaigns_stg = marketing_campaigns_temp_stg.dropDuplicates(['source_campaign_id'])
table_dict['marketing_campaigns'] = marketing_campaigns_stg

consumer_marketing_actions_temp_stg = spark.sql("select name_first, name_last, email, source_campaign_id, marketing_action_type, click_url, message_name, subscription_policy, proxy_source, consumer_ip, created_at from mms where marketing_action_type is not null")
consumer_marketing_actions_temp_stg.createOrReplaceTempView("cmats")
consumers_stg.createOrReplaceTempView("cs")
marketing_campaigns_stg.createOrReplaceTempView("mcs")
consumer_marketing_actions_temp_stg = spark.sql("select distinct(*), uuid() as id from (select cs.id as consumer_id, mcs.id as campaign_id,cmats.marketing_action_type,cmats.click_url,cmats.message_name,cmats.subscription_policy as message_subscription_policy,cmats.proxy_source,cmats.consumer_ip, cmats.email, cmats.created_at from cmats left join mcs on cmats.source_campaign_id == mcs.source_campaign_id join cs on cmats.email == cs.email and cmats.name_first <=> cs.name_first and cmats.name_last <=> cs.name_last)")
marketing_lists_temp_stg = spark.sql("select list_id as source_list_id, list_name, description as list_description, list_type from mms where list_id is not null")
marketing_lists_temp_stg = marketing_lists_temp_stg.dropDuplicates(['source_list_id'])
marketing_lists_temp_stg = marketing_lists_temp_stg.withColumn("id",expr("uuid()"))

campaign_marketing_lists_temp_stg = spark.sql("select *, uuid() as id from (select distinct source_campaign_id, list_id as source_list_id from mms where source_campaign_id is not null)")
campaign_marketing_lists_temp_stg.createOrReplaceTempView('cmlts')
marketing_campaigns_stg.createOrReplaceTempView('mcs')
marketing_lists_temp_stg.createOrReplaceTempView('mlts')
campaign_marketing_lists_stg = spark.sql("select uuid() as id, mcs.id as campaign_id, mlts.id as marketing_list_id from cmlts join mcs on cmlts.source_campaign_id = mcs.source_campaign_id join mlts on cmlts.source_list_id = mlts.source_list_id")
table_dict['campaign_marketing_lists'] = campaign_marketing_lists_stg

marketing_action_list_temp_stg = spark.sql("select list_id as source_list_id, email, created_at from mms where marketing_action_type = 'email_subscribe' or marketing_action_type = 'email_unsubscribe'")
marketing_action_list_temp_stg.createOrReplaceTempView('malts')
consumer_marketing_actions_temp_stg.createOrReplaceTempView('cmats')
marketing_lists_temp_stg.createOrReplaceTempView('mlts')
marketing_action_list_stg = spark.sql("select *, uuid() as id from (select cmats.id as action_id, mlts.id as list_id from malts join cmats on malts.email = cmats.email and malts.created_at = cmats.created_at join mlts on malts.source_list_id = mlts.source_list_id)")
marketing_lists_stg = marketing_lists_temp_stg.drop("source_list_id")
table_dict['consumer_marketing_actions'] = consumer_marketing_actions_temp_stg.drop('email','created_at')
table_dict['marketing_action_list'] = marketing_action_list_stg
table_dict['marketing_lists'] = marketing_lists_stg

table_dict = addCreatedAndUpdatedAt(table_dict)

write_to_ttdm_targets(glueContext,table_dict, args["refined_bucket"], args["redshift_endpoint"], args["db_name"],args["schema_name"],args["username"],args["redshift_password"],args["redshift_role_arn"],args["TempDir"],todays_date)

# for k,v in table_dict.items():
    
#     if table_dict[k].rdd.isEmpty():
#         print('No data for {}'.format(k))
#         continue
#     datamart_table = DynamicFrame.fromDF(table_dict[k], glueContext, k)
    
#     try:
#         glueContext.purge_s3_path('s3://{}/{}/{}/{}'.format(args['refined_bucket'],'datamart', k, todays_date), {"retentionPeriod": 0})
#     except:
#         print("Exception occured in clearing target folder location")
        
#     print('writing {} to s3'.format(k))
    
#     glueContext.write_dynamic_frame.from_options(
#             frame=datamart_table,
#             connection_type='s3',
#             format='json',
#             connection_options={
#                 'path': 's3://{}/{}/{}/{}'.format(args['refined_bucket'],'datamart', k, todays_date)
#             },
#         )
    
#     print('writing {} to Redshift'.format(k))
    
#     my_conn_options = {
#         'dbtable': '{}.{}'.format(args['schema_name'], k),
#         'preactions':'truncate table {}.{}'.format(args['schema_name'],k),
#         'database': args['db_name'],
#         'aws_iam_role': args['redshift_role_arn']}
    
#     glueContext.write_dynamic_frame.from_jdbc_conf(
#         frame=datamart_table,
#         catalog_connection=args['connection_name'],
#         connection_options=my_conn_options,
#         transformation_ctx='write_{}_table'.format(k),
#         redshift_tmp_dir= args['TempDir']
#     )

job.commit()