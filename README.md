serapis
=======

##How to run the project - (on Ubuntu)

1. Install the following:
                                             
        sudo apt-get install python-pip python-dev build-essential
        sudo pip install --upgrade pip
        sudo pip install cython amqp amqplib distribute celery django django-celery mongoengine pymongo simplejson djangorestframework markdown
        sudo apt-get install mongodb-server
        sudo apt-get install rabbitmq-server
        sudo apt-get install curl
        sudo pip install pycurl
        sudo pip install --upgrade pycurl


2. You need a settings.py file - maybe it would be better to use the one I have, for now, and modify the absolute paths from it, with paths on your machine (will fix this in the future).

3. Start a Celery - worker on your machine:

        python manage.py celery worker --loglevel=info

4. Start the Django server:

        python manage.py runserver --noreload

 // you can start it also without to --noreload, but from my experience, the automatic restart of the server didn't work so well. 

5. Open http://127.0.0.1:8000/serapis/upload/ in your browser.



##The functionality - roughly:


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

- models.py - Database model used in the application. Each python class defined here corresponds to a document or to an EmbeddedDocument ( a document embedded within another document). I tried to model the data in such a way, that the fields, most of them reside physically in the same document, but to organise them logically in separate entities: the actual document inherits from DynamicDocument (mongoengine class), while the nested types inherit from DynamicEmbeddedDocument. I thought this might ease the handling of the data from the html forms.


- serapis/templates/serapis - folder containing all the html pages. I have created a base.html class with the elements common for all the pages (e.g. header and footer) => all the .html files that I wish to contain these common elements should extend **base.html** page - by having on the first line of the file:

	{% extends "base.html" %}

In **base.html** one can also declare other blocks that are to be implemented in the html pages that extend the base.
	e.g.:
	{% block content %}
	<!-- Here comes some html...-->
    	{% endblock %}

###Flow:

Request -> url searched within urls.py file -> when an entry from urls.py matches, the corresponding view (function or class) is called, which eventually initializes a form with the data submitted from an html form.


##FAQ


1.    Do I need to have a web-server installed on the machine, where I want to run the project?

    **A:** The answer is: depends if you wish to run the project for development or production purposes. Django comes with a web-server, but that is purely for testing-development purposes. If you want to run the project in production, you should search for a different web-server.

2.    Why do I need class-based views? Why would I want to have a whole class for each view, when I could have only one function?

    **A:** The main reason is that if one needs auxilliary functions to serve a view, then the views.py will become a huge container for both view-functions and auxilliary functions. By having a class for each view, things will look more organized and easier to read for someone who sees the project for the first time => it would be clearer which auxilliary function belongs to which view.
In addition to this, I have chosen class-based views also because, according to the documentation, in the future Django will support only class-based views.

3.    What is a DynamicDocument? Why do all the classes declared in models.py inherit from it?

    **A:** All the documents that we wish to store in the MongoDB database using mongoengine need to inherit from the class Document or other classes inheriting from Document class, like DynamicDocument. Using a DynamicDocument allows us to save in the DB not only the documents with a fixed structure - corresponding to the classes in models.py, but also documents that contain other fields than the ones declared in the document class definition. 
    e.g.:

            class Person(DynamicDocument):
        	    name = StringField()
 

            pers = Person()
            pers.name = 'John'
            pers.job = 'manager'    # new field
            pers.save()	# saves the object in the database with both fields name and job



4.    How do I declare a function as a task to be put in a queue in order to be run on a worker?


    **A:**
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




