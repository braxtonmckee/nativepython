import { createTextInput } from './components/text-input';
import * as maquette from 'maquette';
import io from 'socket.io-client'

// set up a basic socket
var socket = io.connect('http://' + document.domain + ':' + location.port);
socket.on('connect', function() {
	socket.emit('my event', {data: 'I\'m connected!'});
});

// we are sticking with maquette notation and style
var h = maquette.h;


const socketData = {
	tag: "div",
	attrs: {"style": "background-color:blue"},
	children: [
		{
			tag: "div",
			attrs: {"style": "color:red"},
			children: ["a child"]
		},
		{
			tag: "div",
			attrs: {"style": "color:yellow"},
			children: ["another child"]
		},
		{
			tag: "div",
			attrs: {"style": "color:yellow"},
			children: [
				{
					tag: "div",
					attrs: {
						"style": "color:green; text-align:center; font-size: 2rem"},
					children: ["a nested child"]
				}
			]
		}
	]
}

function generate(data) {
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

export function render() {
  return generate(socketData) 
}
