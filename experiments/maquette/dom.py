""" an example of a dict/json dom representation """

socketData = {
    "tag": "div",
    "attrs": {
        "style": "background-color:gray; height: 100%",
    },
    "children": [
        {
            "tag": "div",
            "attrs": {"style": "text-align:center; width:100%"},
            "children": [
                {
                    "tag": "h1",
                    "attrs": {"style": "color:blue"},
                    "children": ["nested child"]
                },
                {
                    "tag": "h1",
                    "attrs": {"style": "color:red"},
                    "children": ["another nested child"]
                }
            ]
        }
    ]
}
