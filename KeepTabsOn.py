#!/usr/bin/env python


# KeepTabsOn
# A Sticky note that's driven by internet hashtags :D

# The MIT License (MIT)

# Copyright (c) 2013 Mevin Babu Chirayath @mevinbabuc

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import httplib2
import logging
import os

from apiclient import discovery
from oauth2client import appengine
from oauth2client import client
from google.appengine.api import memcache
from google.appengine.api import users
from google.appengine.ext import ndb

import webapp2
import jinja2
import json

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

class HashStore(ndb.Model):
    """Models an individual HashStore entry with hastag, tile, and date."""

    author = ndb.UserProperty()
    title = ndb.StringProperty(indexed=False)
    hashtag = ndb.StringProperty(indexed=True, default="")
    viewDate = ndb.DateTimeProperty(auto_now_add=True)

def CSOR_Jsonify(func):
    """ decorator to make all the requests CSOR compatible and jsonfy the output """

    def wrapper(*args, **kw):

        dataOject=func(*args, **kw)

        try:
            _origin = args[0].request.headers['Origin']
        except:
            _origin = "http://gcdc2013-keeptabson.appspot.com/"

        args[0].response.headers.add_header("Access-Control-Allow-Origin", _origin)
        args[0].response.headers.add_header("Access-Control-Allow-Credentials", "true")
        args[0].response.headers.add_header("Access-Control-Allow-Headers", "origin, x-requested-with, content-type, accept")
        args[0].response.headers.add_header('Content-Type', 'application/json')

        args[0].response.write(json.dumps(dataOject))
    return wrapper

class ResT(webapp2.RequestHandler):
    """ Class to handle requests (GET, POST, DELETE) to the route /tag/ . """


    @CSOR_Jsonify
    @decorator.oauth_aware
    def post(self,query=""):
        """ Post Request handler to add data to the HashStore

            args   :None

            return :status
                    status['object'] -> The object that was added to the HashStore with title and hastag
                    status['success']-> Has a boolean value. -> True | False 
                    status['error']  -> Error message if any

            Exceptions/response status codes :201 -> Created    -> When a new object was saved in HashStore
                                              404 -> Not Found  -> When the post variables title and hashtags was blank or NULL

        """

        status={}
        status["error"]=None
        status["success"]=True
        key=False

        NoteTitle = self.request.get("title")
        NoteHashtags = self.request.get("hashtags")

        if NoteHashtags and NoteTitle:
            HashEntry=HashStore(author=users.get_current_user(),hashtag=NoteHashtags,title=NoteTitle)
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
        """ Get request handler to retrieve the list of Tabs saved in the HashStore

            args  :None

            return: dataList -> An object containing all the Tabs of the logged in user.Each tab contains title, hashtag
                                and the date it was created.

            Exceptions/response status codes :404 -> Not Found -> When there's no data in the HashStore for the particular user
                                              400 -> Bad Request -> When the program is unable to search db etc. Try again later.
                                              200 -> Ok -> When data is found and proper data is returned.

        """

        qry = HashStore.query().filter(HashStore.author==users.get_current_user())
        dataList=[]

        if qry :
            for temp in qry:
                dataObject={}
                dataObject["title"]=temp.title
                dataObject["hashtag"]=temp.hashtag
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
        """ Delete request handler to delete a Tab from HashStore

            args   :query -> Accepts tabs(Hashtag) that has to be deleted for the particular user

            return :status -> No value -> Delete request is not supposed to return any value

            response status codes :404 -> Not Found -> When the data to be deleted is not found in the HashStore
                                   204 -> No Content -> When data is found in the HashStore and deleted,so there's no content to return
                                   400 -> Bad Request -> When invalid query( Hashtag) was passed to the delete request

        """

        status={}
        hashtags = query.strip()

        if hashtags:
            qry = HashStore.query().filter(HashStore.author==users.get_current_user(),HashStore.hashtag==hashtags).fetch(keys_only=True)
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
        self.response.headers.add_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
        self.response.headers.add_header("Access-Control-Allow-Credentials", "true")
        self.response.headers.add_header("Access-Control-Allow-Headers", "origin, x-requested-with, content-type, accept")


class ResTSearch(webapp2.RequestHandler):
    """ Class to handle GET request to search google plus using the G+ API. """


    @CSOR_Jsonify
    @decorator.oauth_aware
    def get(self,orderBy,query):
        """ Get request to search google plus for the best and recent results, based on Hashtags.

            args   : orderBy -> accepts two values ,"Best" and "Recent"

            return : TagDataSuper -> Object with search results from GPlus API.

            response status codes : 200 -> Ok -> Found search results for the queries
                                    404 -> Not Found -> No content found for the query provided
                                    400 -> Bad Request -> Either of the arguments is not present.Unable to search.

        """

        TagDataSuper=[]
        if query:
            for eachHashTag in query.split(","):
                kp=decorator.http()
                temp=service.activities().search(query=str(eachHashTag.strip()),orderBy=orderBy,maxResults=20,language="en-GB").execute(http=kp)
                dataList=[]
                if 'items' in temp:
                    #self.response.write('got page with '+str(len( temp['items'] )))
                    for activity in temp['items']:
                        dataObject={}
                        dataObject["post_url"]=activity['url'].encode('utf-8').strip()
                        dataObject["title"]=activity['title'].encode('utf-8').strip()
                        dataObject["date"]=activity['published'].encode('utf-8').strip()
                        dataObject["user"]=activity["actor"]["displayName"].strip()
                        dataObject["user_url"]=activity["actor"]["url"].strip()
                        dataObject["user_img_url"]=activity["actor"]["image"]["url"].strip()
                        dataObject["content"]=activity['object']['content'].encode('utf-8').strip()
                        if 'attachments' in activity['object'] :
                            dataObject["attached_content"]=activity['object']['attachments']
                            # self.response.write(repr(activity['object']).encode('utf-8').strip()+"<br><br><br>")
                        dataList.append(dataObject)
                    TagDataSuper.append(dataList)
            self.response.set_status(200,"Ok")

        if str(TagDataSuper)=='[[]]' or len(TagDataSuper)==0:
            self.response.set_status(404,"Not Found")

        elif not query or not orderBy:
            self.response.set_status(400,"Bad Request")

        else :
            self.response.set_status(200,"Ok")

        return TagDataSuper

    def options(self,query="",orderBy=""):

        try:
            _origin = self.request.headers['Origin']
        except:
            _origin = "http://gcdc2013-keeptabson.appspot.com/"
        
        self.response.set_status(200,"Ok")
        self.response.headers.add_header("Access-Control-Allow-Origin", _origin)
        self.response.headers.add_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
        self.response.headers.add_header("Access-Control-Allow-Credentials", "true")
        self.response.headers.add_header("Access-Control-Allow-Headers", "origin, x-requested-with, content-type, accept")


class MainHandler(webapp2.RequestHandler):


    @decorator.oauth_aware
    def get(self):
        variables = {
            'google_url': decorator.authorize_url(),
            'google': decorator.has_credentials(),
        }
        template = JINJA_ENVIRONMENT.get_template('templates/auth.html')
        self.response.write(template.render(variables))

class login(webapp2.RequestHandler):


    def get(self):
        user = users.get_current_user()

        if user:
            self.redirect("/")
        else:
            self.redirect(users.create_login_url(self.request.uri))


application = webapp2.WSGIApplication(
    [
        webapp2.Route(r'/u/', MainHandler),
        webapp2.Route(r'/tag/<orderBy:best|recent>/<query:.*>', ResTSearch),
        webapp2.Route(r'/tag/<query:.*>', ResT),
        ('/login', login),
        webapp2.Route(decorator.callback_path, decorator.callback_handler()),
        ],
    debug=True)