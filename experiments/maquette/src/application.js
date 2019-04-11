import { createTextInput } from './components/text-input';
import * as maquette from 'maquette';
import io from 'socket.io-client'

var projector = maquette.createProjector();

// set up a basic socket
var socket = io.connect('ws://localhost:5000');
socket.on('connect', function() {
	socket.emit('message', {message: 'I\'m connected!'});
});


// load initial data
var socketData = {
	tag: "div",
	attrs: {id: 'test-id', style: "height:500px"},
	children: ['ok']
};


// we are sticking with maquette notation and style
var h = maquette.h;


function poll() {
    socket.emit("init");
    socket.on("init", function(data) {
        socketData = data;
        
        projector.scheduleRender();
    });
}


function generate(data) {
	if (data === undefined) {
		return h('div', ["no data"]);
	}
	if (data["children"].length == 1){
		let child = data["children"][0];
		if (typeof(child) === "string") {
			return ( 
				h(data["tag"], data["attrs"], data["children"])
			)
		} else {
			return h(data["tag"], data["attrs"], [generate(child)])
		}
	} else {
		let children = data["children"].map((c) => generate(c));
		return ( 
			h(data["tag"], data["attrs"], children)	
		)
	}
}

// Initializes the projector 
document.addEventListener('DOMContentLoaded', function () {
	projector.append(document.body, render);
});

document.addEventListener('DOMContentLoaded', function () {
	projector.replace(document.getElementById("test-id"), render);
    setInterval(poll, 500);
});

export function render() {
	return generate(socketData) 
}
