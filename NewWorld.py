from google.appengine.api import users

import jinja2
import webapp2
import os

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

class login(webapp2.RequestHandler):

    def get(self):
        user = users.get_current_user()

        if user:
            self.response.headers['Content-Type'] = 'text/plain'
            self.response.write('Hello, ' + user.nickname())
        else:
            self.redirect(users.create_login_url(self.request.uri))
        
            

        
class renderPage(webapp2.RequestHandler):
    
    def get(self):
        
        msg="Hello !"
        user=users.get_current_user()
        
        if user:
            msg="Hello, "+str(user.nickname())+"!"
            
        values={ 'msg':msg,   }
        template = JINJA_ENVIRONMENT.get_template('index.html')
        self.response.write(template.render(values))

application = webapp2.WSGIApplication([
    ('/login', login),
    ('/render',renderPage),
], debug=True)
