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
	attrs: {id: 'initial'},
	children: ['ok']
};

socket.emit("init");

// we are sticking with maquette notation and style
var h = maquette.h;

socket.on("init", function(data) {
	console.log('ok');
	console.log(data);
	socketData = data;
	
	projector.scheduleRender();
})

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
	projector.merge(document.body, render);
});

export function render() {
	console.log(socketData);
	return generate(socketData) 
}
