""" an example of a dict/json dom representation """

socketData = {
    "tag": "div",
    "attrs": {
        "style": "background-color:gray; height: 700px",
    },
    "children": [
        {
            "tag": "div",
            "attrs": {
                "style": "text-align:center; width:100%",
                "id": "test-id"
            },
            "children": [
                {
                    "tag": "h1",
                    "attrs": {"style": "color:blue"},
                    "children": ["nested child"]
                },
                {
                    "tag": "h1",
                    "attrs": {"style": "color:black"},
                    "children": ["another nested child"]
                }
            ]
        }
    ]
}
