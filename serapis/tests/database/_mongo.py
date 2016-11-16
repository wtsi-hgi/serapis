import atexit
import logging
from typing import Optional

from docker.errors import APIError

from hgicommon.docker.client import create_client
from hgicommon.docker.models import Container
from hgicommon.helpers import create_random_string, get_open_port

MONGO_DOCKER_REPOSITORY = "mongo"
MONGO_DOCKER_TAG = "3"
MONGO_DEFAULT_PORT = 27017


_docker_client = create_client()


def _get_mongo_docker_image() -> Optional[str]:
    """
    Gets the Mongo Docker image.
    :return: image identifier or `None` if it hasn't been pulled
    """
    identfiers = _docker_client.images("%s:%s" % (MONGO_DOCKER_REPOSITORY, MONGO_DOCKER_TAG), quiet=True)
    return identfiers[0] if len(identfiers) > 0 else None


class MongoContainer(Container):
    """
    Model of a Mongo container.
    """
    def __init__(self):
        super().__init__()
        self.port = None
        self.host = "localhost"


def stop_mongo_container(container: MongoContainer):
    """
    Stops the given Mongo container.
    :param container:
    :return:
    """
    try:
        _docker_client.kill(container.native_object)
    except Exception as e:
        ignore = False
        if isinstance(e, APIError):
            ignore = "is not running" in str(e.explanation)
        if not ignore:
            raise e


def start_mongo_container() -> MongoContainer:
    """
    Starts a Mongo container. Will be killed on exit if it is still running.
    :return: model of the running container
    """
    if len(_get_mongo_docker_image()) == 0:
        # Docker image doesn't exist locally: getting from DockerHub
        _docker_client.pull(MONGO_DOCKER_REPOSITORY, MONGO_DOCKER_TAG)

    container = MongoContainer()
    container.name = create_random_string(prefix="%s-" % MONGO_DOCKER_REPOSITORY)
    container.port = get_open_port()
    container.native_object = _docker_client.create_container(
        image=_get_mongo_docker_image(), name=container.name, ports=[MONGO_DEFAULT_PORT],
        host_config=_docker_client.create_host_config(port_bindings={MONGO_DEFAULT_PORT: container.port}))

    atexit.register(stop_mongo_container, container)
    _docker_client.start(container.native_object)

    for line in _docker_client.logs(container.native_object, stream=True):
        line = str(line)
        logging.debug(line)
        if "waiting for connections on port" in line:
            break

    return container
