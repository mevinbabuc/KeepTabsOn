#!/usr/bin/env python
#
# Copyright 2013 Google Inc.
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
"""Starting template for Google App Engine applications.

Use this project as a starting point if you are just beginning to build a Google
App Engine project. Remember to download the OAuth 2.0 client secrets which can
be obtained from the Developer Console <https://code.google.com/apis/console/>
and save them as 'client_secrets.json' in the project directory.
"""

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

# CLIENT_SECRETS, name of a file containing the OAuth 2.0 information for this
# application, including client_id and client_secret, which are found
# on the API Access tab on the Google APIs
# Console <http://code.google.com/apis/console>
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

# Helpful message to display in the browser if the CLIENT_SECRETS file
# is missing.
MISSING_CLIENT_SECRETS_MESSAGE = """
<h1>Warning: Please configure OAuth 2.0</h1>
<p>
To make this sample run you will need to populate the client_secrets.json file
found at:
</p>
<p>
<code>%s</code>.
</p>
<p>with information found on the <a
href="https://code.google.com/apis/console">APIs Console</a>.
</p>
""" % CLIENT_SECRETS

http = httplib2.Http(memcache)
service = discovery.build('plus', 'v1', http=http)
decorator = appengine.oauth2decorator_from_clientsecrets(
    CLIENT_SECRETS,
    scope=[
      'https://www.googleapis.com/auth/plus.login',
      'https://www.googleapis.com/auth/plus.me',
    ],
    message=MISSING_CLIENT_SECRETS_MESSAGE)


class HashStore(ndb.Model):
    """Models an individual HashStore entry with hastag, content, and date."""
    hastag = ndb.StringProperty(indexed=True)
    content = ndb.StringProperty(indexed=False)
    date = ndb.DateTimeProperty(auto_now_add=True)


class Note(ndb.Model):
    """Models an individual Note entry."""
    author = ndb.UserProperty()
    title = ndb.StringProperty(indexed=False)
    content = ndb.StringProperty(indexed=False)
    hashtag = ndb.StringProperty(repeated=True)
    last_viewed = ndb.DateTimeProperty(auto_now_add=True)



class MainHandler(webapp2.RequestHandler):

  @decorator.oauth_aware
  def get(self):
    variables = {
        'url': decorator.authorize_url(),
        'has_credentials': decorator.has_credentials()
        }
    template = JINJA_ENVIRONMENT.get_template('main.html')
    self.response.write(template.render(variables))

class login(webapp2.RequestHandler):

    def get(self):
        user = users.get_current_user()

        if user:
            self.redirect("/")
        else:
            self.redirect(users.create_login_url(self.request.uri))
        
class Add(webapp2.RequestHandler):

  def post(self):
      NoteTitle = self.request.get("title")
      NoteHashtags = []
      NoteHashtags.append(self.request.get("hashtags"))
      
      HashEntry=HashStore(hastag=str(repr(NoteHashtags)),content="data")
      HashEntry.put()
      
      NoteEntry = Note(author=users.get_current_user(),hashtag=NoteHashtags,title=NoteTitle)
      NoteEntry.content="data"
      NoteEntry.put()


class Remove(webapp2.RequestHandler):

  def post(self):
      noteId = self.request.get("noteId")
      self.response.write(str(noteId))
        
class View(webapp2.RequestHandler):

  def get(self):
      # noteId = self.request.get("noteId")
      
      qry = Note.query().filter(Note.author==users.get_current_user())
      for temp in qry:
          self.response.write(str(temp.title)+" "+str(temp.hashtag)+" "+str(temp.author))

class renderPage(webapp2.RequestHandler):
    
  def get(self):
      
      msg="Hello !"
      user=users.get_current_user()
      
      if user:
          msg="Hello, "+str(user.nickname())+"!"
          
      values={ 'msg':msg,   }
      template = JINJA_ENVIRONMENT.get_template('static/static_html/index.html')
      self.response.write(template.render(values))


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

app = webapp2.WSGIApplication(
    [
      webapp2.Route(r'/', MainHandler),
      webapp2.Route(r'/tag/<orderBy:best|recent>/<query:.*>', TagSearch),
      ('/login', login),
      ('/render',renderPage),
      ('/add',Add),
      ('/remove',Remove),
      ('/view',View),
      (decorator.callback_path, decorator.callback_handler()),
    ],
    debug=True)