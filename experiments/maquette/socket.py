#!/usr/bin/env python3

# basic socket io setup

from flask import Flask, jsonify  # , render_template
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

#  ## globals

socketData = {
    "tag": "div",
    "attrs": {
        "style": "background-color:blue; height: 100%"},
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
def send_body():
    emit('body', jsonify(socketData))


@socketio.on('update')
def send_update():
    emit('update', jsonify(socketData))


if __name__ == '__main__':
    socketio.run(app)
