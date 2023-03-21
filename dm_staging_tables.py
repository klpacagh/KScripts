r = roster.alias('r')
sc = splash_main.alias('sc')
a = axs.alias('a')
sf = salesforce.alias('sf')
ct = coach.alias('ct')
cc = ccms.alias('cc')

# ADDRESSES STAGING

r_address = r.select(col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             col('address_1').cast(StringType()).alias('address_1'),
             col('address_2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country').cast(StringType()).alias('country'),
             col('zip_postal_code').cast(StringType()).alias('postal_code'))

sc_address = sc.select(col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('primary_email').cast(StringType()).alias('email'),
             col('address').cast(StringType()).alias('address_1'),
             lit(None).cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country').cast(StringType()).alias('country'),
             col('zip_code').cast(StringType()).alias('postal_code'))

a_address_a = a.select(col('name_first').cast(StringType()).alias('name_first'),
             col('name_last').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             col('address_1').cast(StringType()).alias('address_1'),
             col('address_2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state_region').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country').cast(StringType()).alias('country'),
             col('postal_code').cast(StringType()).alias('postal_code'))

a_address_b = a.select(col('name_first').cast(StringType()).alias('name_first'),
             col('name_last').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             col('billing_address_1').cast(StringType()).alias('address_1'),
             col('billing_address_2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('billing_city').cast(StringType()).alias('city'),
             col('billing_state_region').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('billing_country').cast(StringType()).alias('country'),
             col('billing_postal_code').cast(StringType()).alias('postal_code'))

a_address = a_address_a.union(a_address_b)

sf_address = sf.select(col('FirstName').cast(StringType()).alias('name_first'),
             col('LastName').cast(StringType()).alias('name_last'),
             col('Email').cast(StringType()).alias('email'),
             col('Address1').cast(StringType()).alias('address_1'),
             col('Address2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('MailingCity').cast(StringType()).alias('city'),
             col('MailingState').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('MailingCountry').cast(StringType()).alias('country'),
             col('MailingPostalCode').cast(StringType()).alias('postal_code'))

ct_address = ct.select(col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             col('address1').cast(StringType()).alias('address_1'),
             col('address2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             lit(None).cast(StringType()).alias('country'),
             col('zip').cast(StringType()).alias('postal_code'))

cc_address = cc.select(col('first_nm').cast(StringType()).alias('name_first'),
             col('last_nm').cast(StringType()).alias('name_last'),
             col('cyber_txt').cast(StringType()).alias('email'),
             col('street1').cast(StringType()).alias('address_1'),
             col('street2').cast(StringType()).alias('address_2'),
             col('street3').cast(StringType()).alias('address_3'),
             col('street4').cast(StringType()).alias('address_4'),
             col('city_nm').cast(StringType()).alias('city'),
             col('state_cd').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country_nm').cast(StringType()).alias('country'),
             col('postal_cd').cast(StringType()).alias('postal_code'))


address_temp_stg = r_address.union(sc_address).union(a_address).union(sf_address).union(ct_address).union(cc_address)
address_temp_stg.count()

address_stg = address_temp_stg.dropDuplicates(['name_first','name_last','email','address_1','address_2','address_3','address_4','city','postal_code']).withColumn('address_id',expr("uuid()"))
address_stg.count()


#ERESTS STAGING

r_interests = r.select(col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('name'),
             lit(None).cast(StringType()).alias('type'),
             lit(None).cast(IntegerType()).alias('parent'),
             lit(None).cast(BooleanType()).alias('email_opt_in'))

sc_interests = sc.select(col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('primary_email').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('name'),
             lit(None).cast(StringType()).alias('type'),
             lit(None).cast(IntegerType()).alias('parent'),
             lit(None).cast(BooleanType()).alias('email_opt_in'))

a_interests= a.select(col('name_first').cast(StringType()).alias('name_first'),
             col('name_last').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('name'),
             lit(None).cast(StringType()).alias('type'),
             lit(None).cast(IntegerType()).alias('parent'),
             col("email_opt_in").cast(BooleanType()).alias('email_opt_in'))


sf_interests = sf.select(col('FirstName').cast(StringType()).alias('name_first'),
             col('LastName').cast(StringType()).alias('name_last'),
             col('Email').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('name'),
             lit(None).cast(StringType()).alias('type'),
             lit(None).cast(IntegerType()).alias('parent'),
             lit(None).cast(BooleanType()).alias('email_opt_in'))

ct_interests = ct.select(col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('name'),
             lit(None).cast(StringType()).alias('type'),
             lit(None).cast(IntegerType()).alias('parent'),
             lit(None).cast(BooleanType()).alias('email_opt_in'))

cc_interests = cc.select(col('first_nm').cast(StringType()).alias('name_first'),
             col('last_nm').cast(StringType()).alias('name_last'),
             col('cyber_txt').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('name'),
             lit(None).cast(StringType()).alias('type'),
             lit(None).cast(IntegerType()).alias('parent'),
             lit(None).cast(BooleanType()).alias('email_opt_in'))


interests_temp_stg = r_interests.union(sc_interests).union(a_interests).union(sf_interests).union(ct_interests).union(cc_interests)
interests_temp_stg.count()

interests_stg =erests_temp_stg.dropDuplicates(['name_first','name_last','email','name','type','parent']).withColumn('interests_id',expr("uuid()"))
interests_stg.count()

# EVENTS STAGING

r_events = r.select(col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('source_event_id'),
             lit(None).cast(StringType()).alias('event_name'),
             lit(None).cast(StringType()).alias('event_description'),
             lit(None).cast(StringType()).alias('venue_name'),
             lit(None).cast(StringType()).alias('start_time'),
             lit(None).cast(StringType()).alias('end_time'),
             lit(None).cast(StringType()).alias('rsvp_open'),
             lit(None).cast(StringType()).alias('wait_list'),
             lit(None).cast(StringType()).alias('rsvp_closed_at'),
             lit(None).cast(StringType()).alias('date_rsvped'),
             lit(None).cast(StringType()).alias('attending'),
             lit(None).cast(StringType()).alias('checked_in'),
             lit(None).cast(StringType()).alias('vip'),
             lit(None).cast(StringType()).alias('waitlist'),
             lit(None).cast(StringType()).alias('ticket_scanned'),
             lit(None).cast(StringType()).alias('ticket_scanned_datetime'),
             col('address_1').cast(StringType()).alias('address_1'),
             col('address_2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country').cast(StringType()).alias('country'),
             col('zip_postal_code').cast(StringType()).alias('postal_code'))


sc_events = sc.select(col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('primary_email').cast(StringType()).alias('email'),
             col('source_event_id').cast(StringType()).alias('source_event_id'),
             col('event_name').cast(StringType()).alias('event_name'),
             col('event_description').cast(StringType()).alias('event_description'),
             col('venue_name').cast(StringType()).alias('venue_name'),
             col('start_time').cast(StringType()).alias('start_time'),
             col('end_time').cast(StringType()).alias('end_time'),
             col('rsvp_open').cast(StringType()).alias('rsvp_open'),
             col('wait_list').cast(StringType()).alias('wait_list'),
             col('rsvp_closed_at').cast(StringType()).alias('rsvp_closed_at'),
             lit(None).cast(StringType()).alias('date_rsvped'),
             lit(None).cast(StringType()).alias('attending'),
             lit(None).cast(StringType()).alias('checked_in'),
             lit(None).cast(StringType()).alias('vip'),
             lit(None).cast(StringType()).alias('waitlist'),
             lit(None).cast(StringType()).alias('ticket_scanned'),
             lit(None).cast(StringType()).alias('ticket_scanned_datetime'),
             col('address').cast(StringType()).alias('address_1'),
             lit(None).cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country').cast(StringType()).alias('country'),
             col('zip_code').cast(StringType()).alias('postal_code'))


a_events = a.select(col('name_first').cast(StringType()).alias('name_first'),
             col('name_last').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             col('event_unique_id').cast(StringType()).alias('source_event_id'),
             col('event_name').cast(StringType()).alias('event_name'),
             lit(None).cast(StringType()).alias('event_description'),
             col('venue_name').cast(StringType()).alias('venue_name'),
             col('event_datetime').cast(StringType()).alias('start_time'),
             lit(None).cast(StringType()).alias('end_time'),
             lit(None).cast(StringType()).alias('rsvp_open'),
             lit(None).cast(StringType()).alias('wait_list'),
             lit(None).cast(StringType()).alias('rsvp_closed_at'),
             lit(None).cast(StringType()).alias('date_rsvped'),
             lit(None).cast(StringType()).alias('attending'),
             lit(None).cast(StringType()).alias('checked_in'),
             lit(None).cast(StringType()).alias('vip'),
             lit(None).cast(StringType()).alias('waitlist'),
             lit(None).cast(StringType()).alias('ticket_scanned'),
             lit(None).cast(StringType()).alias('ticket_scanned_datetime'),
             col('address_1').cast(StringType()).alias('address_1'),
             col('address_2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state_region').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country').cast(StringType()).alias('country'),
             col('postal_code').cast(StringType()).alias('postal_code'))


sf_events = sf.select(col('FirstName').cast(StringType()).alias('name_first'),
             col('LastName').cast(StringType()).alias('name_last'),
             col('Email').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('source_event_id'),
             lit(None).cast(StringType()).alias('event_name'),
             lit(None).cast(StringType()).alias('event_description'),
             lit(None).cast(StringType()).alias('venue_name'),
             lit(None).cast(StringType()).alias('start_time'),
             lit(None).cast(StringType()).alias('end_time'),
             lit(None).cast(StringType()).alias('rsvp_open'),
             lit(None).cast(StringType()).alias('wait_list'),
             lit(None).cast(StringType()).alias('rsvp_closed_at'),
             lit(None).cast(StringType()).alias('date_rsvped'),
             lit(None).cast(StringType()).alias('attending'),
             lit(None).cast(StringType()).alias('checked_in'),
             lit(None).cast(StringType()).alias('vip'),
             lit(None).cast(StringType()).alias('waitlist'),
             lit(None).cast(StringType()).alias('ticket_scanned'),
             lit(None).cast(StringType()).alias('ticket_scanned_datetime'),
             col('Address1').cast(StringType()).alias('address_1'),
             col('Address2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('MailingCity').cast(StringType()).alias('city'),
             col('MailingState').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('MailingCountry').cast(StringType()).alias('country'),
             col('MailingPostalCode').cast(StringType()).alias('postal_code'))

ct_events = ct.select(col('first_name').cast(StringType()).alias('name_first'),
             col('last_name').cast(StringType()).alias('name_last'),
             col('email').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('source_event_id'),
             lit(None).cast(StringType()).alias('event_name'),
             lit(None).cast(StringType()).alias('event_description'),
             lit(None).cast(StringType()).alias('venue_name'),
             lit(None).cast(StringType()).alias('start_time'),
             lit(None).cast(StringType()).alias('end_time'),
             lit(None).cast(StringType()).alias('rsvp_open'),
             lit(None).cast(StringType()).alias('wait_list'),
             lit(None).cast(StringType()).alias('rsvp_closed_at'),
             lit(None).cast(StringType()).alias('date_rsvped'),
             lit(None).cast(StringType()).alias('attending'),
             lit(None).cast(StringType()).alias('checked_in'),
             lit(None).cast(StringType()).alias('vip'),
             lit(None).cast(StringType()).alias('waitlist'),
             lit(None).cast(StringType()).alias('ticket_scanned'),
             lit(None).cast(StringType()).alias('ticket_scanned_datetime'),
             col('address1').cast(StringType()).alias('address_1'),
             col('address2').cast(StringType()).alias('address_2'),
             lit(None).cast(StringType()).alias('address_3'),
             lit(None).cast(StringType()).alias('address_4'),
             col('city').cast(StringType()).alias('city'),
             col('state').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             lit(None).cast(StringType()).alias('country'),
             col('zip').cast(StringType()).alias('postal_code'))

cc_events = cc.select(col('first_nm').cast(StringType()).alias('name_first'),
             col('last_nm').cast(StringType()).alias('name_last'),
             col('cyber_txt').cast(StringType()).alias('email'),
             lit(None).cast(StringType()).alias('source_event_id'),
             lit(None).cast(StringType()).alias('event_name'),
             lit(None).cast(StringType()).alias('event_description'),
             lit(None).cast(StringType()).alias('venue_name'),
             lit(None).cast(StringType()).alias('start_time'),
             lit(None).cast(StringType()).alias('end_time'),
             lit(None).cast(StringType()).alias('rsvp_open'),
             lit(None).cast(StringType()).alias('wait_list'),
             lit(None).cast(StringType()).alias('rsvp_closed_at'),
             lit(None).cast(StringType()).alias('date_rsvped'),
             lit(None).cast(StringType()).alias('attending'),
             lit(None).cast(StringType()).alias('checked_in'),
             lit(None).cast(StringType()).alias('vip'),
             lit(None).cast(StringType()).alias('waitlist'),
             lit(None).cast(StringType()).alias('ticket_scanned'),
             lit(None).cast(StringType()).alias('ticket_scanned_datetime'),
             col('street1').cast(StringType()).alias('address_1'),
             col('street2').cast(StringType()).alias('address_2'),
             col('street3').cast(StringType()).alias('address_3'),
             col('street4').cast(StringType()).alias('address_4'),
             col('city_nm').cast(StringType()).alias('city'),
             col('state_cd').cast(StringType()).alias('state_region'),
             lit(None).cast(StringType()).alias('int_region'),
             col('country_nm').cast(StringType()).alias('country'),
             col('postal_cd').cast(StringType()).alias('postal_code'))
 
events_temp_stg = r_events.union(sc_events).union(a_events).union(sf_events).union(ct_events).union(cc_events)
events_temp_stg.count()

events_stg = events_temp_stg.dropDuplicates().withColumn('events_id',expr("uuid()"))
events_stg.count()

# marketing staging (start with marketing_lists, N:1 to marketing_campaigns and marketing_lists)
# then it expands to include marketing actions - join with CONSUMER_STG later to update CONSUMER_ID fields
# remember, names/emails can be stripped out when creating the tables that have nothing to do with a consumer

name_first 
name_last 
email
campaign_marketing_lists_id
campaign_id
marketing_list_id
marketing_lists_id
list_name
list_description
list_type
marketing_campaigns_id
source_campaign_id
campaign_name
template_id
message_medium
created_by_user_id
updated_by_user_id
campaign_state
campaign_type
consumer_marketing_actinos_id
marketing_action_type
click_url
message_name
message_subscription_policy
template_id
proxy_source