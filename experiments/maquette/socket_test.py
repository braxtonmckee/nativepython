#!/usr/bin/env python3

# basic socket io setup

from flask import Flask, jsonify  # , render_template
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

#  ## globals

socketData1 = {
    "tag": "div",
    "attrs": {
        "style": "background-color:blue; height: 100%",
        "id": "newone"
    },
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

socketData = {
    "tag": "div",
    "attrs": {
        "style": "background-color:blue; height: 100%",
        "id": "newone"
    },
    "children": ["ok this is something"]
}


@socketio.on('message')
def handle_message(message):
    print('received message: ' + str(message))


@socketio.on('init')
def handle_init():
    emit('init', socketData)


if __name__ == '__main__':
    socketio.run(app, debug=True)
