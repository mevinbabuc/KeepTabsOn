from google.appengine.api import users
from google.appengine.ext import ndb

import jinja2
import webapp2
import os

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)


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
        
        HashEntry=HashStore(hastag=NoteHashtags,content="data")
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
        
        qry = Note.query(author=users.get_current_user())
        
        self.response.write(str(qry))

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
    ('/view',View),
], debug=True)
