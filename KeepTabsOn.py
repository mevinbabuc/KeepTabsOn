from google.appengine.api import users
from google.appengine.ext import ndb

import jinja2
import webapp2
import os

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)


​class HashStore(ndb.Model):
    """Models an individual HashStore entry with hastag, content, and date."""
    hastag = ndb.StringProperty(indexed=False)
    content = ndb.StringProperty(indexed=False)
    date = ndb.DateTimeProperty(auto_now_add=True)


​class Data(ndb.Model):
    """Models an individual Guestbook entry with author, content, and date."""
    author = ndb.UserProperty()
    content = ndb.StringProperty(indexed=False)
    date = ndb.DateTimeProperty(auto_now_add=True)


class login(webapp2.RequestHandler):

    def get(self):
        user = users.get_current_user()

        if user:
            self.redirect("/")
        else:
            self.redirect(users.create_login_url(self.request.uri))
        
class Add(webapp2.RequestHandler):

    def put(self):
        title = self.request.get("title")
        hashtags = self.request.get("hashtags")
        self.response.write(str(title)+" "+str(hashtags))

class Remove(webapp2.RequestHandler):

    def delete(self):
        noteId = self.request.get("noteId")
        self.response.write(str(noteId))

class renderPage(webapp2.RequestHandler):
    
    def get(self):
        
        msg="Hello !"
        user=users.get_current_user()
        
        if user:
            msg="Hello, "+str(user.nickname())+"!"
            
        values={ 'msg':msg,   }
        template = JINJA_ENVIRONMENT.get_template('static/static_html/index.html')
        self.response.write(template.render(values))

application = webapp2.WSGIApplication([
    ('/login', login),
    ('/render',renderPage),
    ('/add',Add),
    ('/remove',Remove),
], debug=True)
