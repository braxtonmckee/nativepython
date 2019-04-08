#!/usr/bin/env python3

# basic socket io setup

from flask import Flask, jsonify  # , render_template
from flask_socketio import SocketIO, send, emit

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

#  ## globals

socketData = {
    "tag": "div",
    "attrs": {"style": "background-color:blue"},
    "children": [
        {
            "tag": "div",
            "attrs": {"style": "color:red"},
            "children": ["a child"]
        },
        {
            "tag": "div",
            "attrs": {"style": "color:yellow"},
            "children": ["another child"]
        },
        {
            "tag": "div",
            "attrs": {"style": "color:yellow"},
            "children": [
                {
                    "tag": "div",
                    "attrs": {
                        "style":
                        "color:green; text-align:center; font-size: 2rem"},
                    "children": ["a nested child"]
                }
            ]
        }
    ]
}


@socketio.on('load')
def send_body(json):
    emit('body', jsonify(socketData))


@socketio.on('update')
def send_update(json):
    emit('update', jsonify(socketData))


if __name__ == '__main__':
    socketio.run(app)
