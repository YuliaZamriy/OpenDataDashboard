# import libraries
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import socket


# from credentials import client_id, pxy, prt, pxy_usr, pxy_pw
# from oauth2client.client import OAuth2WebServerFlow, GoogleCredentials
# from oauth2client.service_account import ServiceAccountCredentials
# import httplib2
# from googleapiclient.discovery import build
# import socket
# import socks

socket.setdefaulttimeout(300)


def get_service(api_name, api_version, scopes, key_file_location):
    """Get a service that communicates to a Google API.

    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scopes: A list auth scopes to authorize for the application.
        key_file_location: The path to a valid service account JSON key file.

    Returns:
        A service that is connected to the specified API.
    """

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=scopes)

    service = build(api_name, api_version, credentials=credentials)

    return service


# Define the auth scopes to request.
scope = 'https://www.googleapis.com/auth/analytics.readonly'
key_file_location = 'ga_project-4c6f6cf2ed45.json'

# Authenticate and construct service.
service = get_service(
    api_name='analyticsreporting',
    api_version='v4',
    scopes=[scope],
    key_file_location=key_file_location)
