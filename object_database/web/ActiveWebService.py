#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import logging
import time
import json
import argparse
import traceback
import os
import threading
import gevent.socket
import gevent.queue

from object_database.util import genToken, validateLogLevel
from object_database import ServiceBase, service_schema, Indexed
from object_database.web.AuthPlugin import AuthPluginBase, LdapAuthPlugin
from object_database.web.LoginPlugin import LoginIpPlugin
from object_database.web.ActiveWebServiceSchema import active_webservice_schema

from object_database.web.cells import (
    Subscribed, Sequence, Traceback, Span, Button, Octicon, Main, Cells,
    Card, Text, Padding, Tabs, Table, Clickable, Dropdown, Popover,
    LargePendingDownloadDisplay, MAX_FPS, HeaderBar, SessionState
)

from typed_python import OneOf, TupleOf, ConstDict
from typed_python.Codebase import Codebase as TypedPythonCodebase

from gevent import pywsgi, sleep
from gevent.greenlet import Greenlet
from geventwebsocket.handler import WebSocketHandler

from flask import (
    Flask,
    jsonify,
    make_response,
    redirect,
    request,
    send_from_directory,
)
from flask_sockets import Sockets
from flask_cors import CORS
from flask_login import LoginManager, current_user, login_required


@active_webservice_schema.define
class LoginPlugin:
    name = Indexed(str)
    # auth plugin
    login_plugin_factory = object  # a factory for LoginPluginInterface objects
    auth_plugins = TupleOf(OneOf(None, AuthPluginBase))
    codebase = OneOf(None, service_schema.Codebase)
    config = ConstDict(str, str)


@active_webservice_schema.define
class Configuration:
    service = Indexed(service_schema.Service)

    port = int
    hostname = str

    log_level = int

    login_plugin = OneOf(None, LoginPlugin)


class ActiveWebService(ServiceBase):
    def __init__(self, db, serviceObject, serviceRuntimeConfig):
        ServiceBase.__init__(self, db, serviceObject, serviceRuntimeConfig)
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def configure(db, serviceObject, hostname, port, level_name="INFO"):
        db.subscribeToType(Configuration)

        with db.transaction():
            c = Configuration.lookupAny(service=serviceObject)
            if not c:
                c = Configuration(service=serviceObject)

            c.hostname = hostname
            c.port = port
            c.log_level = logging.getLevelName(level_name)

    @staticmethod
    def setLoginPlugin(db, serviceObject, loginPluginFactory, authPlugins, codebase=None, config=None):
        db.subscribeToType(Configuration)
        db.subscribeToType(LoginPlugin)

        config = config or {}

        with db.transaction():
            c = Configuration.lookupAny(service=serviceObject)
            if not c:
                c = Configuration(service=serviceObject)
            login_plugin = LoginPlugin(
                name="an auth plugin",
                login_plugin_factory=loginPluginFactory,
                auth_plugins=TupleOf(OneOf(None, AuthPluginBase))(authPlugins),
                codebase=codebase,
                config=config
            )
            c.login_plugin = login_plugin

    @staticmethod
    def configureFromCommandline(db, serviceObject, args):
        """Subclasses should take the remaining args from the commandline and configure using them"""
        db.subscribeToType(Configuration)

        parser = argparse.ArgumentParser("Configure a webservice")
        parser.add_argument("--hostname", type=str)
        parser.add_argument("--port", type=int)
        # optional arguments
        parser.add_argument("--log-level", type=str, required=False, default="INFO")

        parser.add_argument("--ldap-hostname", type=str, required=False)
        parser.add_argument("--ldap-base-dn", type=str, required=False)
        parser.add_argument("--ldap-ntlm-domain", type=str, required=False)
        parser.add_argument("--authorized-groups", type=str, required=False, nargs="+")
        parser.add_argument("--company-name", type=str, required=False)

        parsedArgs = parser.parse_args(args)

        with db.transaction():
            c = Configuration.lookupAny(service=serviceObject)
            if not c:
                c = Configuration(service=serviceObject)

            level_name = parsedArgs.log_level.upper()
            level_name = validateLogLevel(level_name, fallback='INFO')

            c.port = parsedArgs.port
            c.hostname = parsedArgs.hostname

            c.log_level = logging.getLevelName(level_name)

        if parsedArgs.ldap_base_dn is not None:
            ActiveWebService.setLoginPlugin(
                db,
                serviceObject,
                LoginIpPlugin,
                [LdapAuthPlugin(
                    parsedArgs.ldap_hostname,
                    parsedArgs.ldap_base_dn,
                    parsedArgs.ldap_ntlm_domain,
                    parsedArgs.authorized_groups
                )],
                config={'company_name': parsedArgs.company_name}
            )

    def initialize(self):
        # dict from session id (cookie really) to a a list of [cells.SessionState]
        self.sessionStates = {}

        self.db.subscribeToType(Configuration)
        self.db.subscribeToType(LoginPlugin)
        self.db.subscribeToSchema(service_schema)

        with self.db.transaction():
            self.app = Flask(__name__)
            CORS(self.app)
            self.sockets = Sockets(self.app)
            self.configureApp()
        self.login_manager = LoginManager(self.app)
        self.login_manager.login_view = 'login'

    def doWork(self, shouldStop):
        self._logger.info("Configuring ActiveWebService")
        with self.db.view() as view:
            config = Configuration.lookupAny(service=self.serviceObject)
            assert config, "No configuration available."
            self._logger.setLevel(config.log_level)
            host, port = config.hostname, config.port

            login_config = config.login_plugin

            codebase = login_config.codebase
            if codebase is None:
                ser_ctx = TypedPythonCodebase.coreSerializationContext()
            else:
                ser_ctx = codebase.instantiate().serializationContext
            view.setSerializationContext(ser_ctx)

            self.login_plugin = login_config.login_plugin_factory(
                self.db, login_config.auth_plugins, login_config.config
            )

            # register `load_user` method with login_manager
            self.login_plugin.load_user = self.login_manager.user_loader(self.login_plugin.load_user)

            self.authorized_groups_text = self.login_plugin.authorized_groups_text

            self.login_plugin.init_app(self.app)

        self._logger.info("ActiveWebService listening on %s:%s", host, port)

        server = pywsgi.WSGIServer(
            (host, port),
            self.app,
            handler_class=WebSocketHandler
        )

        server.serve_forever()

    def configureApp(self):
        self.app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or genToken()

        self.app.add_url_rule('/', endpoint='index', view_func=lambda: redirect("/services"))
        self.app.add_url_rule('/content/<path:path>', endpoint=None, view_func=self.sendContent)
        self.app.add_url_rule('/services', endpoint=None, view_func=self.sendPage)
        self.app.add_url_rule('/services/<path:path>', endpoint=None, view_func=self.sendPage)
        self.app.add_url_rule('/status', view_func=self.statusPage)
        self.sockets.add_url_rule('/socket/<path:path>', None, self.mainSocket)

    def statusPage(self):
        return make_response(jsonify("STATUS: service is up"))

    @login_required
    def sendPage(self, path=None):
        self._logger.info("Sending 'page.html'")
        return self.sendContent("page.html")

    def mainDisplay(self):
        def serviceCountSetter(service, ct):
            def f():
                service.target_count = ct
            return f

        serviceCounts = list(range(5)) + list(range(10, 100, 10)) + list(range(100, 400, 25)) + list(range(400, 1001, 100))

        buttons = Sequence([
            Padding(),
            Button(
                Sequence([Octicon('shield').color('green'), Span('Lock ALL')]),
                lambda: [s.lock() for s in service_schema.Service.lookupAll()]),
            Button(
                Sequence([Octicon('shield').color('orange'), Span('Prepare ALL')]),
                lambda: [s.prepare() for s in service_schema.Service.lookupAll()]),
            Button(
                Sequence([Octicon('stop').color('red'), Span('Unlock ALL')]),
                lambda: [s.unlock() for s in service_schema.Service.lookupAll()]),
        ])
        tabs = Tabs(
            Services=Table(
                colFun=lambda: [
                    'Service', 'Codebase Status', 'Codebase', 'Module', 'Class',
                    'Placement', 'Active', 'TargetCount', 'Cores', 'RAM', 'Boot Status'],
                rowFun=lambda:
                    sorted(service_schema.Service.lookupAll(), key=lambda s: s.name),
                headerFun=lambda x: x,
                rendererFun=lambda s, field: Subscribed(
                    lambda:
                    Clickable(s.name, "/services/" + s.name) if field == 'Service' else
                    (   Clickable(Sequence([Octicon('stop').color('red'), Span('Unlocked')]),
                                  lambda: s.lock()) if s.isUnlocked else
                        Clickable(Sequence([Octicon('shield').color('green'), Span('Locked')]),
                                  lambda: s.prepare()) if s.isLocked else
                        Clickable(Sequence([Octicon('shield').color('orange'), Span('Prepared')]),
                                  lambda: s.unlock())) if field == 'Codebase Status' else
                    (str(s.codebase) if s.codebase else "") if field == 'Codebase' else
                    s.service_module_name if field == 'Module' else
                    s.service_class_name if field == 'Class' else
                    s.placement if field == 'Placement' else
                    Subscribed(
                        lambda: len(service_schema.ServiceInstance.lookupAll(service=s))
                    ) if field == 'Active' else
                    Dropdown(
                        s.target_count,
                        [(str(ct), serviceCountSetter(s, ct)) for ct in serviceCounts]
                    ) if field == 'TargetCount' else
                    str(s.coresUsed) if field == 'Cores' else
                    str(s.gbRamUsed) if field == 'RAM' else
                    (
                        Popover(
                            Octicon("alert"),
                            "Failed",
                            Traceback(s.lastFailureReason or "<Unknown>")
                        ) if s.isThrottled() else ""
                    ) if field == 'Boot Status' else
                    ""
                ),
                maxRowsPerPage=50
            ),
            Hosts=Table(
                colFun=lambda: [
                    'Connection', 'IsMaster', 'Hostname', 'RAM ALLOCATION',
                    'CORE ALLOCATION', 'SERVICE COUNT', 'CPU USE', 'RAM USE'
                ],
                rowFun=lambda: sorted(service_schema.ServiceHost.lookupAll(), key=lambda s: s.hostname),
                headerFun=lambda x: x,
                rendererFun=lambda s, field: Subscribed(
                    lambda:
                    s.connection._identity if field == "Connection" else
                    str(s.isMaster) if field == "IsMaster" else
                    s.hostname if field == "Hostname" else
                    "%.1f / %.1f" % (s.gbRamUsed, s.maxGbRam) if field == "RAM ALLOCATION" else
                    "%s / %s" % (s.coresUsed, s.maxCores) if field == "CORE ALLOCATION" else
                    str(len(service_schema.ServiceInstance.lookupAll(host=s))) if field == "SERVICE COUNT" else
                    "%2.1f" % (s.cpuUse * 100) + "%" if field == "CPU USE" else
                    ("%2.1f" % s.actualMemoryUseGB) + " GB" if field == "RAM USE" else
                    ""
                ),
                maxRowsPerPage=50
            )
        )
        return Sequence([buttons, tabs])

    def displayForPathAndQueryArgs(self, path, queryArgs):
        display, toggles = self.displayAndHeadersForPathAndQueryArgs(path, queryArgs)
        return self.addMainBar(display, toggles)

    def displayAndHeadersForPathAndQueryArgs(self, path, queryArgs):
        if len(path) and path[0] == 'services':
            if len(path) == 1:
                return self.mainDisplay(), []

            serviceObj = service_schema.Service.lookupAny(name=path[1])

            if serviceObj is None:
                return Traceback("Unknown service %s" % path[1]), []

            serviceType = serviceObj.instantiateServiceType()

            serviceToggles = [
                x.withSerializationContext(serviceObj.getSerializationContext())
                for x in serviceType.serviceHeaderToggles(serviceObj)
            ]

            if len(path) == 2:
                return (
                    Subscribed(lambda: serviceType.serviceDisplay(serviceObj, queryArgs=queryArgs))
                    .withSerializationContext(serviceObj.getSerializationContext()),
                    serviceToggles
                )

            typename = path[2]

            schemas = serviceObj.findModuleSchemas()
            typeObj = None
            for s in schemas:
                typeObj = s.lookupFullyQualifiedTypeByName(typename)
                if typeObj:
                    break

            if typeObj is None:
                return Traceback("Can't find fully-qualified type %s" % typename), []

            if len(path) == 3:
                return (
                    serviceType.serviceDisplay(serviceObj, objType=typename, queryArgs=queryArgs)
                    .withSerializationContext(serviceObj.getSerializationContext()),
                    serviceToggles
                )

            try:
                instance = typeObj.fromIdentity(int(path[3]))
            except ValueError:
                return Traceback("Invalid object identity")

            return (
                serviceType.serviceDisplay(serviceObj, instance=instance, queryArgs=queryArgs)
                .withSerializationContext(serviceObj.getSerializationContext()),
                serviceToggles
            )

        return Traceback("Invalid url path: %s" % path), []

    def addMainBar(self, display, toggles):
        current_username = current_user.username

        return (
            HeaderBar(
                [
                    Subscribed(
                        lambda: Dropdown(
                            "Service",
                            [("All", "/services")] +
                            [(s.name, "/services/" + s.name)
                             for s in sorted(service_schema.Service.lookupAll(), key=lambda s:s.name)]
                        ),
                    )
                ],
                toggles,
                [
                    LargePendingDownloadDisplay(),
                    Octicon('person') + Span(current_username),
                    Span('Authorized Groups: {}'.format(self.authorized_groups_text)),
                    Button(Octicon('sign-out'), '/logout')
                ]) +
            Main(display)
        )

    @login_required
    def mainSocket(self, ws, path):
        path = str(path).split("/")
        queryArgs = dict(request.args.items())

        sessionId = request.cookies.get("session")

        # wait for the other socket to close if we were bounced
        sleep(.25)

        if sessionId is None:
            sessionState = SessionState()
        else:
            # we keep sessions in a list. This is not great, but if you
            # bounce your browser, you'll get the session state you just dropped.
            # if you have several windows open, close a few, and then reopen
            # you'll get a random one
            sessionStateList = self.sessionStates.setdefault(sessionId, [])
            if not sessionStateList:
                self._logger.info("Creating a new SessionState for %s", sessionId)
                sessionState = SessionState()
            else:
                sessionState = sessionStateList.pop()

        self._logger.info("entering websocket with path %s", path)
        reader = None
        isFirstMessage = True

        try:
            self._logger.info("Starting main websocket handler with %s", ws)

            cells = Cells(self.db)

            # reset the session state. There's only one per cells (which is why
            # we keep a list of sessions.)
            sessionState._reset(cells)

            cells = cells.withRoot(
                Subscribed(lambda: self.displayForPathAndQueryArgs(path, queryArgs)),
                serialization_context=self.db.serializationContext,
                session_state=sessionState
            )

            timestamps = []

            lastDumpTimestamp = time.time()
            lastDumpMessages = 0
            lastDumpFrames = 0
            lastDumpTimeSpentCalculating = 0.0

            FRAME_SIZE = 32 * 1024
            FRAMES_PER_ACK = 10  # this HAS to line up with the constant in page.html for our ad-hoc protocol to function.

            # large messages (more than FRAMES_PER_ACK frames) send an ack after every FRAMES_PER_ACKth message
            largeMessageAck = gevent.queue.Queue()

            def readThread():
                while not ws.closed:
                    msg = ws.receive()
                    if msg is None:
                        return
                    else:
                        try:
                            jsonMsg = json.loads(msg)
                            if 'ACK' in jsonMsg:
                                largeMessageAck.put(jsonMsg['ACK'])
                            else:
                                cell_id = jsonMsg.get('target_cell')
                                cell = cells[cell_id]
                                if cell is not None:
                                    cell.onMessageWithTransaction(jsonMsg)
                        except Exception:
                            self._logger.error("Exception in inbound message: %s", traceback.format_exc())

                        cells.triggerIfHasDirty()

                largeMessageAck.put(StopIteration)

            reader = Greenlet.spawn(readThread)

            def writeJsonMessage(message):
                """Send a message over the websocket. We have to chunk in 64kb frames
                to keep the websocket from disconnecting on chrome for very large messages.
                This appears to be a bug in the implementation?
                """
                msg = json.dumps(message)

                # split msg int 64kb frames
                frames = []
                i = 0
                while i < len(msg):
                    frames.append(msg[i:i+FRAME_SIZE])
                    i += FRAME_SIZE

                if len(frames) >= FRAMES_PER_ACK:
                    self._logger.info("Sending large message of %s bytes over %s frames", len(msg), len(frames))

                ws.send(json.dumps(len(frames)))

                for index, frame in enumerate(frames):
                    ws.send(frame)

                    # block until we get the ack for FRAMES_PER_ACK frames ago. That way we always
                    # have FRAMES_PER_ACK frames in the buffer.
                    framesSent = index+1
                    if framesSent % FRAMES_PER_ACK == 0 and framesSent > FRAMES_PER_ACK:
                        ack = largeMessageAck.get()
                        if ack is StopIteration:
                            return
                        else:
                            assert ack == framesSent - FRAMES_PER_ACK, (ack, framesSent - FRAMES_PER_ACK)

                framesSent = len(frames)

                if framesSent >= FRAMES_PER_ACK:
                    finalAckIx = framesSent - (framesSent % FRAMES_PER_ACK)

                    ack = largeMessageAck.get()
                    if ack is StopIteration:
                        return
                    else:
                        assert ack == finalAckIx, (ack, finalAckIx)

            while not ws.closed:
                t0 = time.time()
                messages = cells.renderMessages()

                user = self.login_plugin.load_user(current_user.username)
                if not user.is_authenticated:
                    ws.close()
                    return

                lastDumpTimeSpentCalculating += time.time() - t0

                if isFirstMessage:
                    self._logger.info("Completed first rendering loop")
                    isFirstMessage = False

                for message in messages:
                    gevent.socket.wait_write(ws.stream.handler.socket.fileno())

                    writeJsonMessage(message)

                    lastDumpMessages += 1

                lastDumpFrames += 1
                if time.time() - lastDumpTimestamp > 60.0:
                    self._logger.info(
                        "In the last %.2f seconds, spent %.2f seconds calculating %s messages over %s frames",
                        time.time() - lastDumpTimestamp,
                        lastDumpTimeSpentCalculating,
                        lastDumpMessages,
                        lastDumpFrames
                    )

                    lastDumpFrames = 0
                    lastDumpMessages = 0
                    lastDumpTimeSpentCalculating = 0
                    lastDumpTimestamp = time.time()

                # tell the browser to execute the postscripts that its built up.
                writeJsonMessage("postscripts")

                cells.wait()

                timestamps.append(time.time())

                if len(timestamps) > MAX_FPS:
                    timestamps = timestamps[-MAX_FPS+1:]
                    if (time.time() - timestamps[0]) < 1.0:
                        sleep(1.0 / MAX_FPS + .001)

        except Exception:
            self._logger.error("Websocket handler error: %s", traceback.format_exc())
        finally:
            self.sessionStates[sessionId].append(sessionState)

            self._logger.info(
                "Returning session state to pool for %s. Have %s",
                sessionId,
                len(self.sessionStates[sessionId])
            )

            if reader:
                reader.join()

    @login_required
    def echoSocket(self, ws):
        while not ws.closed:
            message = ws.receive()
            if message is not None:
                ws.send(message)

    @login_required
    def sendContent(self, path):
        own_dir = os.path.dirname(__file__)
        return send_from_directory(os.path.join(own_dir, "content"), path)

    @staticmethod
    def serviceDisplay(serviceObject, instance=None, objType=None, queryArgs=None):
        c = Configuration.lookupAny(service=serviceObject)

        return Card(Text("Host: " + c.hostname) + Text("Port: " + str(c.port)))
