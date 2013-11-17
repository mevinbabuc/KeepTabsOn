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
<h1>ERROR: I'm broke now :(</h1>
<p>
Server Error :(
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
    hastag = ndb.StringProperty(indexed=False, default="")
    viewDate = ndb.DateTimeProperty(auto_now_add=True)

class Add(webapp2.RequestHandler):

    def post(self):
        NoteTitle = self.request.get("title")
        NoteHashtags = self.request.get("hashtags")

        HashEntry=HashStore(author=users.get_current_user(),hashtag=NoteHashtags,title=NoteTitle)
        HashEntry.put()

        status={}
        status["error"]=None
        status["success"]=True

        self.response.headers['Content-Type'] = 'application/json' 
        self.response.write(json.dumps(status))

class View(webapp2.RequestHandler):

    def get(self):
        # noteId = self.request.get("noteId")

        qry = HashStore.query().filter(HashStore.author==users.get_current_user())
        dataList=[]
        for temp in qry:
            dataObject={}
            dataObject["title"]=temp.title
            dataObject["hashtag"]=temp.hashtag
            dataObject["viewDate"]=temp.date

            dataList.append(dataObject)

        self.response.headers['Content-Type'] = 'application/json' 
        self.response.write(json.dumps(dataList))

class Remove(webapp2.RequestHandler):

    def post(self):
        hashtags = self.request.get("hashtags")
        qry = HashStore.query().filter(HashStore.author==users.get_current_user(),HashStore.hashtag==hashtags).fetch(keys_only=True)
        ndb.delete_multi(qry)

        status={}
        status["error"]=None
        status["success"]=True

        self.response.headers['Content-Type'] = 'application/json' 
        self.response.write(json.dumps(status))

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

class TagSearch(webapp2.RequestHandler):
  
  @decorator.oauth_aware
  def get(self,orderBy,query):

    TagDataSuper=[]
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
    self.response.headers['Content-Type'] = 'application/json' 
    self.response.write(json.dumps(TagDataSuper))

application = webapp2.WSGIApplication(
    [
        webapp2.Route(r'/u/', MainHandler),
        webapp2.Route(r'/tag/<orderBy:best|recent>/<query:.*>', TagSearch),
        ('/login', login),
        ('/add',Add),
        ('/remove',Remove),
        ('/view',View),
        webapp2.Route(decorator.callback_path, decorator.callback_handler()),
    ],
    debug=True)