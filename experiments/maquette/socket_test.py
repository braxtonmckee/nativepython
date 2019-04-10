#!/usr/bin/env python3

# basic socket io setup

from flask import Flask, jsonify  # , render_template
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)


@socketio.on('message')
def handle_message(message):
    print('received message: ' + str(message))


@socketio.on('init')
def handle_init():
    import dom
    emit('init', dom.socketData)


if __name__ == '__main__':
    socketio.run(app, debug=True)
