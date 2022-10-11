import pandas as pd
import phonenumbers
from phonenumbers import timezone

df = pd.read_csv('phone_numbers.csv')


def phone_check(phone):
    try:
        parse_phone = phonenumbers.parse(str(phone[0]))
        if phonenumbers.is_possible_number(parse_phone):
            return "valid phone"
    except:
        return "invalid phone" 

def phone_get_time_zone(phone):
    try:
        parse_phone = phonenumbers.parse(phone)
        if phonenumbers.is_possible_number(parse_phone):
            time_zone = timezone.time_zones_for_number(parse_phone)
            return time_zone
    except:
        return "invalid tz" 


df['validate'] = df.apply(phone_check, axis = 1)
df['timezone'] = df['phone'].apply(phone_get_time_zone)

print('\nAfter Applying Function: ')
print(df)