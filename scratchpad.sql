match_sql = spark.sql("select trim(BOTH ' \t' FROM stg.address_1) as stg_address_1, trim(BOTH ' \t' FROM rs.address_1) as rs_address_1, stg.id, rs.id \
                       from redshift_addresses rs join staging_addresses stg on (rs.address_1 = stg.address_1 AND rs.address_2 = stg.address_2 AND rs.address_3 = stg.address_3 AND rs.address_4 = stg.address_4 AND rs.city = stg.city AND rs.state_region = stg.state_region AND rs.country = stg.country AND rs.postal_code = stg.postal_code)")

--568904
match_sql = spark.sql("select stg.address_1 as stg_address_1, rs.address_1 as rs_address_1, stg.address_2 as stg_address_2 , rs.address_2 as rs_address_2 , stg.id, rs.id from redshift_addresses rs join staging_addresses stg on (trim(BOTH ' \t' FROM rs.address_1) = trim(BOTH ' \t' FROM stg.address_1))")
match_sql.count()


match_sql = spark.sql("select stg.address_1 as stg_address_1, rs.address_1 as rs_address_1, stg.address_2 as stg_address_2 , rs.address_2 as rs_address_2 , stg.id, rs.id from redshift_addresses rs join staging_addresses stg on (trim(BOTH ' \t' FROM rs.address_1) = trim(BOTH ' \t' FROM stg.address_1) AND rs.city = stg.city AND rs.state_region = stg.state_region AND rs.country = stg.country AND rs.postal_code = stg.postal_code)")
match_sql.count()