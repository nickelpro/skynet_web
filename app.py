import web
from handlers import root_handler, category_handler, object_handler

class hello:
	def GET(self, *args):
		return "Hello"

mapping = (
	('/', 'endpoints'), 
	('/([a-zA-Z0-9_]*)', 'category_handler'),
	('/([a-zA-Z0-9_]*)\.([a-zA-Z0-9_]*)', 'category_handler'),
	('/([a-zA-Z0-9_]*)/([a-zA-Z0-9_]*)', 'object_handler'),
	('/([a-zA-Z0-9_]*)/([a-zA-Z0-9_]*)\.([a-zA-Z0-9_]*)', 'object_handler'),
	)

fvars = {
	'endpoints': root_handler,
	'category_handler': category_handler,
	'object_handler': object_handler,
	}

app = web.application()
app.mapping = mapping
app.fvars = fvars

if __name__ == "__main__":
	app.run()