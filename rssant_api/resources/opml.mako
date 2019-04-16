<?xml version="1.0"?>
<opml version="1.0">
    <head>
        <title>Feeds from RSSAnt</title>
    </head>
    <body>
    % for feed in feeds:
        <outline text="${ feed['link'] }" title="${ feed['title'] }" type="${ feed['version'] }" xmlUrl="${ feed['url'] }"/>
    % endfor
    </body>
</opml>