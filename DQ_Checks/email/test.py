import re

#Return a match at every word character (characters from a to Z, digits from 0-9, and the underscore _ character):

email = "tfd@pga.com.invalid"

# find the rightmost period

top_level_domains = ["com","org","net","int","edu","gov","mil"]
main_match = re.search(r'[\w.]+@[\w.]', str(email)) 

index_of_period = email.rindex(".") + 1
domain_sub = email[index_of_period:]
if domain_sub in top_level_domains and main_match:
  print("valid email")
else:
  print("not valid")