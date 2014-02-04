#!/usr/bin/env python
#
# KeepTabsOn
# A Sticky note that's driven by internet hashtags :D
#
# Copyright 2013 Mevin Babu Chirayath <mevinbabuc@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import httplib2
import logging
import os
import re
import json


from google.appengine.api import memcache
from google.appengine.api import users
from google.appengine.ext import ndb
from google.appengine.api import urlfetch

from apiclient import discovery
from oauth2client import appengine
from oauth2client import client

import webapp2
from webapp2_extras import sessions
import jinja2

import sys
sys.path.insert(0, 'tweepy/')
import tweepy

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    autoescape=True,
    extensions=['jinja2.ext.autoescape'])

CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

MISSING_CLIENT_SECRETS_MESSAGE = """
<h1>ERROR: :| </h1>
<p>
Try back again later.
</p>
"""

http = httplib2.Http(memcache)
service = discovery.build('plus', 'v1', http=http)
decorator = appengine.oauth2decorator_from_clientsecrets(
    CLIENT_SECRETS,
    scope=[
        'https://www.googleapis.com/auth/plus.me',
        ],
    message=MISSING_CLIENT_SECRETS_MESSAGE)


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

class OAuthToken(ndb.Model):
    """Model to hold Oauth token for GPlus"""

    token_key = ndb.StringProperty(required=True)
    token_secret = ndb.StringProperty(required=True)

class HashStore(ndb.Model):
    """Models an individual HashStore entry with hastag, tile, and date."""

    author = ndb.UserProperty()
    title = ndb.StringProperty(indexed=False)
    hashtag = ndb.StringProperty(indexed=True, default="")
    viewDate = ndb.DateTimeProperty(auto_now_add=True)

def CSOR_Jsonify(func):
    """ decorator to make all requests CSOR compatible and jsonfy the output """

    def wrapper(*args, **kw):

        dataOject=func(*args, **kw)

        try:
            _origin = args[0].request.headers['Origin']
        except:
            _origin = "http://gcdc2013-keeptabson.appspot.com/"

        args[0].response.headers.add_header("Access-Control-Allow-Origin", _origin)
        args[0].response.headers.add_header("Access-Control-Allow-Credentials", "true")
        args[0].response.headers.add_header("Access-Control-Allow-Headers",
         "origin, x-requested-with, content-type, accept")
        args[0].response.headers.add_header('Content-Type', 'application/json')

        args[0].response.write(json.dumps(dataOject))
    return wrapper

def HTML_Strip(html=""):
    return re.sub('<[^<]+?>', '', html).strip()

##############################################################################

class ResT(webapp2.RequestHandler):
    """ Class to handle requests (GET, POST, DELETE) to the route /tag/ . """


    @CSOR_Jsonify
    @decorator.oauth_aware
    def post(self,query=""):
        """Post Request handler to add data to the HashStore

        Args:

        return:
            A status object which contains the data added and error messages if any.
            status['object']
            status['success']
            status['error']

        Exceptions/response status codes :
            201 -> Created   -> When a new object was saved in HashStore
            404 -> Not Found -> When the post variables title and hashtags was
                                blank or NULL

        """

        status={}
        status["error"]=None
        status["success"]=True
        key=False

        NoteTitle = self.request.get("title")
        NoteHashtags = self.request.get("hashtags")

        if NoteHashtags and NoteTitle:
            HashEntry=HashStore(author=users.get_current_user(),
                hashtag=NoteHashtags,title=NoteTitle)
            key=HashEntry.put()
            self.response.set_status(201,"Created")
            status['Object']={"title":NoteTitle,"hashtag":NoteHashtags}

        if not key:
            status["success"]=False
            status["error"]="Unable to Add your Tab.Try again"
            self.response.set_status(404,"Not Found")

        return status

    @CSOR_Jsonify
    @decorator.oauth_aware
    def get(self,query=""):
        """Get request handler to retrieve the list of Tabs saved in the HashStore

        Args:

        Return:
            An object containing all the Tabs of the logged in user.Each tab 
            contains title, hashtag and the date it was created.

        Response status codes :
            404 -> Not Found -> When there's no data in the HashStore for the
                                particular user
            400 -> Bad Request->When the program is unable to search db etc.
                                Try again later.
            200 -> Ok -> When data is found and proper data is returned.

        """

        qry = HashStore.query().filter(HashStore.author==users.get_current_user())
        dataList=[]

        if qry :
            for temp in qry:
                dataObject={}
                dataObject["title"]=temp.title
                dataObject["hashtags"]=temp.hashtag
                dataObject["viewDate"]=temp.viewDate.strftime("%Y/%m/%d %H:%M")

                dataList.append(dataObject)

        if len(dataList)==0:
            self.response.set_status(404,"Not Found")
        elif not qry :
            self.response.set_status(400,"Bad Request")
        else :
            self.response.set_status(200,"Ok")


        return dataList

    @CSOR_Jsonify
    @decorator.oauth_aware
    def delete(self,query):
        """Delete request handler to delete a Tab from HashStore

        Args:
            query: Accepts tabs(Hashtag) that has to be deleted for the 
            particular user

        Return:
            Delete request is not supposed to return any value

        Response status codes :
            404 -> Not Found -> When the data to be deleted is not found in the 
                                HashStore
            204 -> No Content-> When data is found in the HashStore and deleted,
                                so there's no content to return
            400 -> Bad Request->When invalid query( Hashtag) was passed to the 
                                delete request

        """

        status={}
        hashtags = query.strip()

        if hashtags:
            qry = HashStore.query().filter(
                HashStore.author==users.get_current_user(),
                HashStore.hashtag==hashtags).fetch(keys_only=True)

            ndb.delete_multi(qry)

            if not qry:
                self.response.set_status(404,"Not Found")
            else :
                self.response.set_status(204,"No Content")

        if not hashtags:
            self.response.set_status(400,"Bad Request")

        return status

    def options(self,query=""):

        try:
            _origin = self.request.headers['Origin']
        except:
            _origin = "http://gcdc2013-keeptabson.appspot.com/"

        self.response.set_status(200,"Ok")
        self.response.headers.add_header("Access-Control-Allow-Origin", _origin)
        self.response.headers.add_header("Access-Control-Allow-Methods",
         "GET, POST, OPTIONS, PUT, DELETE")
        self.response.headers.add_header("Access-Control-Allow-Credentials", "true")
        self.response.headers.add_header("Access-Control-Allow-Headers",
         "origin, x-requested-with, content-type, accept")

#/tag/best/<Query>
class ResTSearch(SessionHandler):
    """Class to handle GET request to search google plus using the G+ API. """


    @CSOR_Jsonify
    @decorator.oauth_aware
    def get(self,orderBy,query):
        """Get request to retrieve and combine data from Twitter and
        Google Plus Search.

        Args:
            orderBy -> accepts two values ,"Best" and "Recent"

        Return:
            returns an object TagDataSuper with search results from GPlus API.

        Response status codes:
            200 -> Ok -> Found search results for the queries
            404 -> Not Found -> No content found for the query provided
            400 -> Bad Request -> Either of the arguments is not present.Unable to search.

        """
        node = self.request.path_info.split('/')
        if node[-1] == '':
            node.pop(-1)

        if node[1] == 't':
            return self.TwitterSearch(orderBy,query)

        if node[1] == 'g':
            return self.GplusSearch(orderBy,query)

        if node[1] == 'tag':
            a = self.TwitterSearch(orderBy,query)
            b = self.GplusSearch(orderBy,query)

            return a + b

    @decorator.oauth_aware
    def TwitterSearch(self,orderBy,query):

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

            return TagDataSuper
        else:
            return self.redirect('/twitoauth/')        

    @decorator.oauth_aware
    def GplusSearch(self,orderBy,query):

        TagDataSuper=[]
        if query:
            for eachHashTag in query.split(","):
                kp=decorator.http()
                temp=service.activities().search(query=str(eachHashTag.strip()),
                    orderBy=orderBy, maxResults=20, 
                    language="en-GB").execute(http=kp)

                dataList=[]
                if 'items' in temp:
                    for activity in temp['items']:
                        dataObject={}
                        dataObject["post_url"]=activity['url'].encode('utf-8').strip()
                        dataObject["title"]=activity['title'].encode('utf-8').strip()
                        dataObject["date"]=activity['published'].encode('utf-8').strip()
                        dataObject["user"]=activity["actor"]["displayName"].strip()
                        dataObject["user_url"]=activity["actor"]["url"].strip()
                        dataObject["user_img_url"]=activity["actor"]["image"]["url"].strip()
                        dataObject["content"]=HTML_Strip(activity['object']['content'].encode('utf-8').strip())

                        metadata={}
                        metadata['replies'] = activity['object']['replies']['totalItems']
                        metadata['plusoners'] = activity['object']['plusoners']['totalItems']
                        metadata['resharers'] = activity['object']['resharers']['totalItems']

                        dataObject['metadata'] = metadata

                        if 'attachments' in activity['object'] :
                            dataObject["attached_content"]=activity['object']['attachments']
                        dataList.append(dataObject)
                    TagDataSuper.extend(dataList)
            self.response.set_status(200,"Ok")

        if str(TagDataSuper)=='[[]]' or len(TagDataSuper)==0:
            self.response.set_status(404,"Not Found")

        elif not query or not orderBy:
            self.response.set_status(400,"Bad Request")

        else :
            self.response.set_status(200,"Ok")

        return TagDataSuper        

    def options(self,orderBy="",query=""):

        try:
            _origin = self.request.headers['Origin']
        except:
            _origin = "http://gcdc2013-keeptabson.appspot.com/"

        self.response.set_status(200,"Ok")
        self.response.headers.add_header("Access-Control-Allow-Origin", _origin)
        self.response.headers.add_header("Access-Control-Allow-Methods",
         "GET")
        self.response.headers.add_header("Access-Control-Allow-Credentials", "true")
        self.response.headers.add_header("Access-Control-Allow-Headers",
         "origin, x-requested-with, content-type, accept")
###############################################################################

class GplusOauth(webapp2.RequestHandler):


    @decorator.oauth_aware
    def get(self):
        context = {
            'google_url': decorator.authorize_url(),
            'google': decorator.has_credentials(),
            'twitter_url':None,
        }
        template = JINJA_ENVIRONMENT.get_template('templates/auth.html')
        self.response.write(template.render(context))

###############################################################################


CONSUMER_KEY = 'heqMmsf4eLA8RtmyIhu1w'
CONSUMER_SECRET = '7SNHt57hQVmT6O9yaiFY1m5jjSO4o6t5x0A1Ll65Tg'
# CALLBACK = 'http://gcdc2013-keeptabson.appspot.com/twitoauth/callback'
CALLBACK = 'http://127.0.0.1:8080/twitoauth/callback'

# OAuth request handler  (/twitoauth/)
class TwitOauth(webapp2.RequestHandler):

    def get(self):
        context = {}
        # Build a new oauth handler and display authorization url to user.
        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET, CALLBACK)
        try:
            context = {

                    "twitter_url": auth.get_authorization_url(),
                    'google_url': None,
                    'google': None,
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

        template = JINJA_ENVIRONMENT.get_template('templates/auth.html')
        self.response.write(template.render(context))

# Callback page (/twitoauth/callback)
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
            # We doautho not seem to have this request token, show an error.
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
        
        self.response.write("Twitter Authorized. Now you can enjoy the power of twitter too.")


###############################################################################

class login(webapp2.RequestHandler):


    def get(self):
        user = users.get_current_user()

        if user:
            self.redirect("/")
        else:
            self.redirect(users.create_login_url(self.request.uri))

config={}
config['webapp2_extras.sessions'] = {'secret_key': 'this~is!my#session^super*key',}

application = webapp2.WSGIApplication(
    [
        ('/login', login),
        webapp2.Route(r'/tag/<orderBy:best|recent>/<query:.*>', ResTSearch),
        webapp2.Route(r'/tag/<query:.*>', ResT),

        # oAuth for G+
        webapp2.Route(r'/gplusoauth/', GplusOauth),
        webapp2.Route(r'/g/<orderBy:best|recent>/<query:.*>', ResTSearch),


        # OAuth for twitter
        (r'/twitoauth/', TwitOauth),
        (r'/twitoauth/callback', CallbackPage),
        webapp2.Route(r'/t/<orderBy:best|recent>/<query:.*>', ResTSearch),

        webapp2.Route(decorator.callback_path, decorator.callback_handler()),
        ],
        config=config,
        debug=True)