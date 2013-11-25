#!/usr/bin/env python
#

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

#Hashtag datastore
class HashStore(ndb.Model):
    """Models an individual HashStore entry with hastag, tile, and date."""
    author = ndb.UserProperty()
    title = ndb.StringProperty(indexed=False)
    hashtag = ndb.StringProperty(indexed=True, default="")
    viewDate = ndb.DateTimeProperty(auto_now_add=True)

def CSOR_Jsonify(func):
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


#Rest processing for KeepTabsOn
class ResT(webapp2.RequestHandler):

    #add objects to datastore

    @CSOR_Jsonify
    @decorator.oauth_aware
    def post(self,query=""):

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


    #view the list of objects in datastore
    
    @CSOR_Jsonify
    @decorator.oauth_aware
    def get(self,query=""):

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

    #delete objects from datstore

    @CSOR_Jsonify
    @decorator.oauth_aware
    def delete(self,query):

        status={}

        hashtags = query.strip()


        if hashtags:
            qry = HashStore.query().filter(HashStore.author==users.get_current_user(),HashStore.hashtag==hashtags).fetch(keys_only=True)
            ndb.delete_multi(qry)

            if not qry:
                self.response.set_status(404,"Not Found")
            else :
                self.response.set_status(200,"Ok")

        if not hashtags:
            self.response.set_status(204,"No Content")

        return status

    def options(self,query=""):
        self.response.set_status(200,"Ok")

        try:
            _origin = self.request.headers['Origin']
        except:
            _origin = "http://gcdc2013-keeptabson.appspot.com/"
            
        self.response.headers.add_header("Access-Control-Allow-Origin", _origin)
        self.response.headers.add_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
        self.response.headers.add_header("Access-Control-Allow-Credentials", "true")
        self.response.headers.add_header("Access-Control-Allow-Headers", "origin, x-requested-with, content-type, accept")


#Rest output for processing for Search Request
class ResTSearch(webapp2.RequestHandler):

    #search function using
    
    @CSOR_Jsonify
    @decorator.oauth_aware
    def get(self,orderBy,query):

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

        self.response.set_status(200,"Ok")

        try:
            _origin = self.request.headers['Origin']
        except:
            _origin = "http://gcdc2013-keeptabson.appspot.com/"
            
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