import pickle
from google.appengine.ext.webapp import RequestHandler, template
from google.appengine.ext import db
import tweepy

from twitsrv.models import OAuthToken

CONSUMER_KEY = 'heqMmsf4eLA8RtmyIhu1w'
CONSUMER_SECRET = '7SNHt57hQVmT6O9yaiFY1m5jjSO4o6t5x0A1Ll65Tg'
CALLBACK = 'https://gcdc2013-keeptabson.appspot.com/oauth/callback'

# Main page handler  (/oauth/)
class MainPage(RequestHandler):

    def get(self):
        context = {}
        # Build a new oauth handler and display authorization url to user.
        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET, CALLBACK)
        try:
            context = {
                    "authurl": auth.get_authorization_url(),
                    "request_token": auth.request_token
            }
        except tweepy.TweepError, e:
            # Failed to get a request token
            return self.response.write("Tweepy error"+str(e))

        # We must store the request token for later use in the callback page.
        request_token = OAuthToken(
                token_key = auth.request_token.key,
                token_secret = auth.request_token.secret
        )
        request_token.put()

        self.response.write(template.render('twitsrv/main.html', context))

# Callback page (/oauth/callback)
class CallbackPage(RequestHandler):

    def get(self):
        oauth_token = self.request.get("oauth_token", None)
        oauth_verifier = self.request.get("oauth_verifier", None)
        if oauth_token is None:
            # Invalid request!
            return self.response.write("invalid callback request")

        # Lookup the request token
        request_token = OAuthToken.gql("WHERE token_key=:key", key=oauth_token).get()
        if request_token is None:
            # We do not seem to have this request token, show an error.
            return self.response.write("Invalid token.Token not in db")

        # Rebuild the auth handler
        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_request_token(request_token.token_key, request_token.token_secret)

        # Fetch the access token
        try:
            auth.get_access_token(oauth_verifier)
        except tweepy.TweepError, e:
            # Failed to get access token
            return self.response.write("Tweepy error"+str(e))

        # So now we could use this auth handler.
        # Here we will just display the access token key&secret
        self.response.write(template.render('twitsrv/callback.html', {
            'access_token': auth.access_token
        }))

