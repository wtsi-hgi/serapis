serapis-web
===========

How to run the project - (on Ubuntu)

1. Install the following:
                                             
- sudo apt-get install python-pip python-dev build-essential
- sudo pip install --upgrade pip
- sudo pip install cython amqp amqplib celery distribute django-celery mongoengine pymongo simplejson
- sudo apt-get install mongodb-server
- sudo apt-get install rabbitmq-server

2. You need a settings.py file - maybe it would be better to use the one I have, for now, and modify the absolute paths from it, with paths on your machine (will fix this in the future).

3. Start a Celery - worker on your machine:

	python manage.py celery worker --loglevel=info

4. Start the Django server:

	python manage.py runserver --noreload

 // you can start it also without to --noreload, but from my experience, the automatic restart of the server didn't work so well. 

5. Open http://127.0.0.1:8000/serapis/upload/ in your browser.



Roughly functionality:


This is how things work in Django and Celery according to my understanding.

Django manages the frontend (view) and communicates with the database (the model), which has been given in settings.py. Celery is in charge of managing the execution of tasks that come from the Django (distribute the tasks to workers and gather the results from the workers back), using RabbitMQ as its broker.

Browser -> web server (which acts like the Celery-server client) -> saves data in the MongoDB and submits further tasks to be executed remotely (or not) by workers


- serapis/urls.py - contains url() functions called with 3 parameters:
		* regex to match the url from request
		* the name of the function (from views.py file or view_classes.py file) to be called when a request for that url comes in
		* (optional) name of that url chosen (to distinguish easier between different urls).

- views.py - contains functions that get as parameter a request and decide what to do with it further and what HttpResponse to return
- views_classes.py - has the same "logical" purpose as views.py, the only difference is that the functions to be called are encapsulated within classes (the framework offers both options - views as functions or as classes, the developer chooses which one he likes
		- each class has a 
				* template_name = field which tells which one is the corresponding html page for that view
				* form_class = field that tells which one is the corresponding form that gets initialized from that html-page-form
				* success_url = url to be used when/if the request is successful



- forms.py - contains forms as classes (that inherit forms.Form)
	 - the fields declared in this kind of classes correspond to fields from the  html page form

- tasks.py - file containing the tasks (defined as functions) to be queued and executed on other machines by the workers (workers = processes/threads started on one or more machines, listening to tasks 'coming' from RabbitMQ queues).

- models.py - Database model used in the application. Each python class defined here corresponds to a document or to an EmbeddedDocument ( a document embedded within another document). I tried to put almost everything in a Mongo document, by logically nesting them (with the help of embedded documents) - to ease the handling of the data from the html forms.

Flow:

Request -> url searched within urls.py file -> when an entry from urls.py matches, the corresponding view (function or class) is called, which eventually initializes a form with the data submitted from an html form.


FAQ

1. How do I declare a function as a task to be put in a queue in order to be run on a worker?


 - Any function can be run on a worker, but we have to tell the framework which one => annotate that function with @task() annotation:
	@task()
	def double(x):
	    result = 2 * int(x)
	    return result
 - call the function from your machine like this:
	import tasks 	# import python file containing the functions annotated with @task
	tasks.parse_BAM_header.delay(bamfile))	# call function delay on the function to be executed remotely 
						# => this tells the framework that this function is to be put in the rabbitMQ worker queue
						# bamfile is the parameter of the function to be executed remotely
 - in order to get the results after the execution of the function above, call get() like:
	result = tasks.parse_BAM_header.delay(bamfile))

2. Do I need to have a web-server installed on the machine where I want to run the project?

A: The answer is: depends if you wish to run the project for development or production. Django comes with a web-server, but that is purely for testing-development purposes. If you want to run the project in production, you should search for a different web-server.



