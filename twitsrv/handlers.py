import json
import re

from google.appengine.ext.webapp import RequestHandler, template
from google.appengine.ext import db

import webapp2
from webapp2_extras import sessions
import tweepy


from twitsrv.models import OAuthToken

CONSUMER_KEY = 'heqMmsf4eLA8RtmyIhu1w'
CONSUMER_SECRET = '7SNHt57hQVmT6O9yaiFY1m5jjSO4o6t5x0A1Ll65Tg'
CALLBACK = 'http://127.0.0.1:9080/oauth/callback'

class SessionHandler(webapp2.RequestHandler):

    def dispatch(self):
        self.session_store = sessions.get_store(request=self.request)
        try:
            webapp2.RequestHandler.dispatch(self)
        finally:
            self.session_store.save_sessions(self.response)

    @webapp2.cached_property
    def session(self):
        return self.session_store.get_session(backend="datastore")


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
class CallbackPage(SessionHandler):

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
        _OAUTHobj = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        _OAUTHobj.set_request_token(request_token.token_key, request_token.token_secret)

        # Fetch the access token
        try:
            _OAUTHobj.get_access_token(oauth_verifier)
        except tweepy.TweepError, e:
            # Failed to get access token
            return self.response.write("Tweepy error"+str(e))

        self.session['_OAUTHobj'] = _OAUTHobj

        # So now we could use this auth handler.
        # Here we will just display the access token key&secret
        # self.response.write(template.render('twitsrv/callback.html', {
        #     'access_token': _OAUTHobj.access_token
        # }))
        
        self.response.write("Twitter Authorized. Now you can enjoy the power of twitter too.")

class TwitterSearch(SessionHandler):

    def get(self,orderBy,query="No Data"):
        _OAUTHobj = self.session.get('_OAUTHobj')

        if _OAUTHobj:
            api = tweepy.API(_OAUTHobj)

            if orderBy == 'best':
                orderBy = 'popular'
            search_result = api.search(q=query,result_type=orderBy)

            TagDataSuper=[]

            for each in search_result:
                dataObject={}
                dataObject["post_url"] = "https://twitter.com/"+each.user.screen_name+"/status/"+each.id_str
                dataObject["title"] = re.sub(r'#[\S]+|(http|https)://[\S]+|@[\S]+','',each.text).strip()
                dataObject["date"] = each.created_at.strftime("%Y/%m/%d %H:%M")
                dataObject["user"] = each.user.screen_name
                dataObject["user_url"] = "https://twitter.com/account/redirect_by_id/"+each.user.id_str
                dataObject["user_img_url"] = each.user.profile_image_url
                dataObject['content']= each.text

                URLdic={}
                URLStack = []
                for each_url in each.entities['urls']:
                    URLStack.append(each_url['expanded_url'])
                URLdic['turl'] = URLStack
                dataObject["attached_content"] = URLdic

                metadata={}
                metadata['retweet'] = each.retweet_count
                metadata['favourites_count'] = each.favorite_count
                metadata['verified'] = each.user.verified
                dataObject['metadata'] = metadata

                TagDataSuper.append(dataObject)
            self.response.headers.add_header('Content-Type', 'application/json')
            return self.response.write(json.dumps(TagDataSuper))
        else:
            return self.redirect('/oauth/')