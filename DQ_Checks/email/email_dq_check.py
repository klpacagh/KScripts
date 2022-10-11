import pandas as pd
# import re
from email_validator import validate_email, EmailNotValidError

df = pd.read_csv('emails.csv')


def email_check(em):
    '''
    match = re.search(r'[\w.-]+@[\w.-]+.\w+', str(em))
    if match:
        return "valid email"
    else:
        return "not valid"
    '''
    try:
        emailObject = validate_email(str(em[0]))
        # If the `testEmail` is valid
        # it is updated with its normalized form
        testEmail = emailObject.email
        return "valid email"
    except EmailNotValidError as errorMsg:
        # If `testEmail` is not valid
        return str(errorMsg)

df['validate'] = df.apply(email_check, axis = 1)

print('\nAfter Applying Function: ')
print(df)