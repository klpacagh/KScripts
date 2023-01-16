create database pga_analytics_db;
create schema pga;

drop table pga.profiles;
drop table pga.emails;
drop table pga.consumers;
drop table pga.consumer_interests;
drop table pga.interests;
drop table pga.consumer_addresses;
drop table pga.addresses;
drop table pga.orders;
drop table pga.order_items;
drop table pga.sources;

CREATE TABLE pga.profiles (
id varchar,
name_first varchar,
name_last varchar,
birth_date date,
gender varchar,
parent_of_junior bool,
league varchar,
team varchar,
created_at timestamp,
updated_at timestamp,
PRIMARY KEY (id)
);

CREATE TABLE pga.emails (
id varchar,
email varchar,
created_at timestamp,
updated_at timestamp,
PRIMARY KEY (id)
);

CREATE TABLE pga.consumers ( 
id varchar,
profile_id int,
email_id int,
do_not_sell bool,
total_ticket_spend float8,
total_merch_spend float8,
total_pga_spend float8,
secondary_purchaser bool,
secondary_seller bool,
trade_in_purchaser bool,
travel_purchaser bool,
merch_purchase_likely float8,
ticket_purchase_likely float8,
internal bool,
vip_status bool,
pga_professional_id varchar,
golf_handicap int,
dma_region varchar,
course_home varchar,
has_coach bool,
volunteer_history varchar,
sign_up_at timestamp,
sign_up_source varchar,
created_at timestamp,
updated_at timestamp,
PRIMARY KEY (id)
);

CREATE TABLE pga.consumer_interests (
id varchar,
consumer_id int,
interest_id int,
email_opt_in bool,
created_at timestamp,
updated_at timestamp,
PRIMARY KEY (id)
);

CREATE TABLE pga.interests (
id varchar,
name varchar,
type varchar,
parent int,
created_at timestamp,
updated_at timestamp,
PRIMARY KEY (id)
);

CREATE TABLE pga.consumer_addresses (
id varchar,
consumer_id int,
address_id int,
created_at timestamp,
updated_at timestamp,
PRIMARY KEY (id)
);

CREATE TABLE pga.addresses (
id varchar,
address_1 varchar,
address_2 varchar,
address_3 varchar,
address_4 varchar,
city varchar,
state_region varchar,
int_region varchar,
country varchar,
postal_code varchar,
created_at timestamp,
updated_at timestamp,
PRIMARY KEY (id)
);

CREATE TABLE pga.orders (
id varchar,
consumer_id int,
source_id int,
source_order_id varchar,
address_shipping_id int,
address_billing_id int,
campaign varchar,
site varchar,
total_discount float8,
total_gross_sub float8,
total_taxable_sub float8,
total_non_taxable_sub float8,
total_tax float8,
total_ship float8,
total float8,
created_at timestamp,
updated_at timestamp,
PRIMARY KEY (id)
);

CREATE TABLE pga.order_items (
id varchar,
order_id int,
product_id varchar,
quantity float8,
unit_price float8,
item_discount float8,
category varchar,
sub_category varchar,
sub_class varchar,
variation_size varchar,
created_at timestamp,
updated_at timestamp,
PRIMARY KEY (id)
);

CREATE TABLE pga.sources (
id varchar,
name varchar,
url varchar,
platform varchar,
PRIMARY KEY (id)
);