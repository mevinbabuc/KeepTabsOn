from google.appengine.api import users
import webapp2
import jinja2

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

class LandingPage(webapp2.RequestHandler):

    def get(self):
        user=users.get_current_user()
        msg=""
        
        if user:
            msg="Hello, !"+user.nickname+" !"
        else :
            self.redirect(users.create_login_url(self.request.uri))
        
        values={ 'msg':msg,   }
        template = JINJA_ENVIRONMENT.get_template('index.html')
        self.response.write(template.render(values))


application = webapp2.WSGIApplication([
    ('/', LandingPage),
], debug=True)
