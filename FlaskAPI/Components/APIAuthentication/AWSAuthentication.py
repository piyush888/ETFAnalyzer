import json
import time
import urllib.request
from jose import jwk, jwt
from jose.utils import base64url_decode

region = 'us-east-1'
userpool_id = 'us-east-1_rLN94MOu0'
app_client_id = '5fhruc7d6tfo0o1kr41ltfass5'
keys_url = 'https://cognito-idp.{}.amazonaws.com/{}/.well-known/jwks.json'.format(region, userpool_id)
# instead of re-downloading the public keys every time
# we download them only on cold start
# https://aws.amazon.com/blogs/compute/container-reuse-in-lambda/
with urllib.request.urlopen(keys_url) as f:
    response = f.read()
keys = json.loads(response.decode('utf-8'))['keys']


def lambda_handler(event, context):
    token = event['token']
    # get the kid from the headers prior to verification
    headers = jwt.get_unverified_headers(token)
    kid = headers['kid']
    # search for the kid in the downloaded public keys
    key_index = -1
    for i in range(len(keys)):
        if kid == keys[i]['kid']:
            key_index = i
            break
    if key_index == -1:
        print('Public key not found in jwks.json')
        return False
    # construct the public key
    public_key = jwk.construct(keys[key_index])
    # get the last two sections of the token,
    # message and signature (encoded in base64)
    message, encoded_signature = str(token).rsplit('.', 1)
    # decode the signature
    decoded_signature = base64url_decode(encoded_signature.encode('utf-8'))
    # verify the signature
    if not public_key.verify(message.encode("utf8"), decoded_signature):
        print('Signature verification failed')
        return False
    print('Signature successfully verified')
    # since we passed the verification, we can now safely
    # use the unverified claims
    claims = jwt.get_unverified_claims(token)
    # additionally we can verify the token expiration
    # if time.time() > claims['exp']:
    #     print('Token is expired')
    #     return False
    # and the Audience  (use claims['client_id'] if verifying an access token)
    if claims['aud'] != app_client_id:
        print('Token was not issued for this audience')
        return False
    # now we can use the claims
    print(claims)
    return claims


# the following is useful to make this script executable in both
# AWS Lambda and any other local environments
if __name__ == '__main__':
    # for testing locally you can enter the JWT ID Token here
    event = {'token': 'eyJraWQiOiJJaDlpNlFGdzF5UExNMFJzdUlyWGVBbFdKeHRTdXhmWXY4Nk54UE9mM0x3PSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiIwMmUwMjAzMi1kODkwLTQ0MjYtOTc4MC01YjgwMGFjMDJlNTMiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnVzLWVhc3QtMS5hbWF6b25hd3MuY29tXC91cy1lYXN0LTFfckxOOTRNT3UwIiwicGhvbmVfbnVtYmVyX3ZlcmlmaWVkIjpmYWxzZSwiY29nbml0bzp1c2VybmFtZSI6IjAyZTAyMDMyLWQ4OTAtNDQyNi05NzgwLTViODAwYWMwMmU1MyIsImF1ZCI6IjVmaHJ1YzdkNnRmbzBvMWtyNDFsdGZhc3M1IiwiZXZlbnRfaWQiOiIzYjIxY2E1MS02YzlkLTQzMDAtOWEwMi01NTUzMDZmM2ZjNjIiLCJ0b2tlbl91c2UiOiJpZCIsImF1dGhfdGltZSI6MTU5MzY5MDUwNiwibmFtZSI6IlBJWVVTSCBHQVJHIiwicGhvbmVfbnVtYmVyIjoiKzkxOTk2MjczNDgzNSIsImV4cCI6MTU5MzY5NDEwNiwiaWF0IjoxNTkzNjkwNTA3LCJlbWFpbCI6InBpeXVzaDg4OEBnbWFpbC5jb20ifQ.nJ2d8ilJgWiAL1oNxejNlISv_cZNtwmBYPc4qVZXEorMGBD1x96LPHBeYNVVyZGdjqe_1NAPGbFUmNYDT7_u5ZyINhTJaB88MBRmvhW62nbaORnA0cuiMh5BLW5wZ3fbGK33kIdV70Ox7lVbx8FC7WMSR6fuvcmKmVu1cLiECC-dUm8juS1-dQjMSwPbm7Ktd6eC5dAFx0SQgWhEdh02bOt8uMoJfZF2NIk84iSSfbIsICMDy-ssIizqUrPeswlynEK4uarCorEJqW6_Es05bzMvPN8wATEvgCpgGmZbqZ79fixn2_y6-4A0uOOC1ClyBPpjhUMHFEGFhz6hqIWY6Q'}
    lambda_handler(event, None)