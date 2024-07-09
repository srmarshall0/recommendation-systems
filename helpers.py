import os
import urllib.parse
import random
import string
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import datetime


class SpotifyClient:
    """
    Client to wrap all Spotify API operation in for ease of use
    """

    def __init__(self, client_id, client_secret, redirect_uri, refresh_token=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.refresh_token = refresh_token
        self.access_token = None

    def _update_access_token(self):
        """
        Use an existing refresh token to get and updated access token instead
        of completing the full authorization flow.
        """
        try:
            res = requests.post(
                url="https://accounts.spotify.com/api/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                },
                auth=HTTPBasicAuth(
                    username=self.client_id, password=self.client_secret
                ),
            )
            return res.json()["access_token"]
        except Exception as e:
            print(f"Error refreshing access token: {e}")
            return None

    def authorize_client(self, scopes):
        """
        Authorize the client to perform actions on a users behalf

        TODO: Add some logic that checks if a refresh token is available to avoid authenticating fully every time

        Args:
            - scopes (str): List of space-separated operations the application is allowed to perform on the users behalf

        Returns:
            - access_token (str)
            - refresh_token (str)
        """

        if not self.refresh_token:
            print("Beginning full authorization flow...")
            # construct an auth url for the user
            state = "".join(
                random.choice(string.ascii_letters + string.digits) for _ in range(16)
            )
            query_params = {
                "response_type": "code",
                "client_id": self.client_id,
                "scope": scopes,
                "redirect_uri": self.redirect_uri,
                "state": state,
            }
            auth_url = f"https://accounts.spotify.com/authorize?{urllib.parse.urlencode(query_params)}"

            # prompt user to visit url
            print(
                "Authenticate at the following URL making sure to copy the URL you are redirected to for the next step."
            )
            print(f"- {auth_url}")

            # retrieve redirect URL and parse params
            redirect_url = input("Paste the redirect URL from the previous step here: ")
            parsed_url = urllib.parse.urlparse(redirect_url)
            url_params = urllib.parse.parse_qs(parsed_url.query)

            # get tokens
            if url_params["state"][0] != state:
                print("Invalid state.")
            else:
                res = requests.post(
                    url="https://accounts.spotify.com/api/token",
                    headers={"content-type": "application/x-www-form-urlencoded"},
                    data={
                        "grant_type": "authorization_code",
                        "code": url_params["code"][0],
                        "redirect_uri": os.getenv("SP_REDIRECT_URI"),
                    },
                    auth=HTTPBasicAuth(
                        username=os.getenv("SP_CLIENT_ID"),
                        password=os.getenv("SP_CLIENT_SECRET"),
                    ),
                )
                data = res.json()

                # set attributes
                self.access_token = data["access_token"]
                self.refresh_token = data["refresh_token"]

                return {
                    "status_code": 200,
                    "message": "client authorization successful!",
                }
        else:
            print("Refreshing access token...")
            self.access_token = self._update_access_token()
            message = "Access token refreshed successfully"
            return {"status_code": 200, "message": message}

    def read_user_library(self):
        """
        Retreive Spotify IDs for all items a user has saved to their library

        Returns:
            - spotify_ids (list): List of a user's saved track's Spotify IDs
        """
        # submit first request
        res = requests.get(
            url="https://api.spotify.com/v1/me/tracks?limit=50",
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        # convert to json
        data = res.json()
        # get next link
        next_url = data["next"]
        # instantiate list to hold ids
        spotify_ids = []
        # move through available items
        while True:
            # get spotify ids for each of the 50 items returned
            for item in data["items"]:
                spotify_id = item["track"]["id"]
                spotify_ids.append(spotify_id)
            # break out of the loop if no next_url is found
            if not next_url:
                break
            # submit another request using the extracted next url
            res = requests.get(
                url=next_url, headers={"Authorization": f"Bearer {self.access_token}"}
            )
            # extract data and next url
            data = res.json()
            next_url = data["next"]
        return spotify_ids

    def get_audio_features(self, ids):
        """
        Get audio features for a given track using provided Spotify IDs

        Args:
            - ids (list): List of Spotify IDs

        Returns:
            - features_df (pd.DataFrame): DataFrame of track features
        """

        # set initial start and end indices
        start_idx = 0
        end_idx = 100
        # instantaite dataframe to hold features
        features_df = pd.DataFrame()
        # loop over ids and get their audio features
        while start_idx < len(ids):
            # create subset of 100 ids
            sub_ids = ids[start_idx:end_idx]
            # convert to string for api
            str_ids = ",".join(sub_ids)
            # submit request for track features
            res = requests.get(
                url=f"https://api.spotify.com/v1/audio-features?ids={str_ids}",
                headers={"Authorization": f"Bearer {self.access_token}"},
            )
            # extract features
            features = res.json()["audio_features"]
            # convert to df
            df = pd.DataFrame(features)
            # stack into big dataframe
            features_df = pd.concat([features_df, df])
            # update start and end values
            start_idx = end_idx
            end_idx = start_idx + 100
            # if were at the end, set end index to the final item
            if end_idx > len(ids):
                end_idx = len(ids)
        return features_df

    def get_track_info(self, id):
        """
        Request track information from Spotify API
        """

        # submit api request
        res = requests.get(
            url=f"https://api.spotify.com/v1/tracks/{id}",
            headers={"Authorization": f"Bearer {self.access_token}"},
        )

        # extract data
        data = res.json()

        return data

    def get_listening_history(self, start_date):
        """
        Retreive Spotify IDs for all items a user listened to starting at the provided
        date up to the current day

        Args:
            - start_date (str): Start date in the format MM/DD/YY

        Returns:
            - spotify_ids (list): List of a user's saved track's Spotify IDs
            - timestamps (list): List of timestamps associated with each ID
        """

        # get timestamp
        # unix_timestamp = self._get_unix_timestamp(start_date)
        date = datetime.datetime.strptime(start_date, "%m/%d/%y")
        unix_timestamp = int(date.timestamp())

        # submit first request
        res = requests.get(
            url=f"https://api.spotify.com/v1/me/player/recently-played?limit=50&after={unix_timestamp}",
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        # convert to json
        data = res.json()
        # get next link
        next_url = data["next"]
        # instantiate list to hold ids
        spotify_ids = []
        timestamps = []
        # move through available items
        while True:
            # get spotify ids for each of the 50 items returned
            for item in data["items"]:
                spotify_id = item["track"]["id"]
                timestamp = item["played_at"]
                spotify_ids.append(spotify_id)
                timestamps.append(timestamp)
            # break out of the loop if no next_url is found
            if not next_url:
                break
            # submit another request using the extracted next url
            res = requests.get(
                url=next_url, headers={"Authorization": f"Bearer {self.access_token}"}
            )
            # extract data and next url
            data = res.json()
            next_url = data["next"]

        return spotify_ids, timestamps
