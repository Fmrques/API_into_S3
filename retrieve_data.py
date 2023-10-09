import requests
import json
import boto3
from credentials import credentials

#AWS and SPOTIFY credentials.
secrets  = {
    'SPOTIFY_ID': credentials.get("SPOTIFY_ID"),
    'SPOTIFY_SECRET': credentials.get("SPOTIFY_SECRET"),
    'AWS_KEY_ID': credentials.get("AWS_KEY_ID"),
    'AWS_KEY_SECRET': credentials.get("AWS_KEY_SECRET"),
    'AWS_BUCKET_NAME': credentials.get("AWS_BUCKET_NAME")
}

spotify_id = secrets['SPOTIFY_ID']
spotify_secret = secrets['SPOTIFY_SECRET']
aws_access_key_id = secrets['AWS_KEY_ID']
aws_secret_access_key = secrets['AWS_KEY_SECRET']
bucket_name = secrets['AWS_BUCKET_NAME']



#GET spotify TOKEN for API requests:
def get_spotify_token(client_id, client_secret):
    # Spotify API endpoint for obtaining an access token
    spotify_token_url = 'https://accounts.spotify.com/api/token'

    # Payload for the POST request
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }

    # Making the POST request
    response = requests.post(spotify_token_url, data=payload, stream=True)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        response_json = response.json()
        print('Token request succeeded!')
    else:
        print('Token request failed!')

    return response_json['access_token']

#GET data request on spotify API:
def retrieve_json_response(token, url):

    #headers of API:
    headers = {
        'Authorization': f'Bearer {token}'
    }
    #Request on spotify API
    response = requests.get(url, headers=headers, stream=True)

    return response

#get spotify token
spotify_token = get_spotify_token(spotify_id, spotify_secret)

#offset is used for pagination in API requests.
offset = 0

#Url of the GET request -- artists with the genre ROCK
url = f'https://api.spotify.com/v1/search?query=genre%3Arock&type=artist&offset={offset}&limit=50'

#open connection to AWS S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

#Defined offset below 1050 just for testing, to get 20 pages maximum.
while offset < 1050:
    try:
        #get response from API and transform it to text to be ingested as txt file in S3 bucket.
        response = retrieve_json_response(spotify_token, url)
        response_txt = json.dumps(response.json())

        if response.status_code == 200:
            key = f'spotify_api/json_{offset}.txt'
            #Load file into S3 as text file in json format
            s3.put_object(Body=response_txt, Bucket=bucket_name, Key=key)
            print(f"File {key} loaded successfully!")
        else:
            print(f"File {key} failed to load.")
        offset += 50
    except Exception as e:
        print(f"An error occurred: {e}")
        break