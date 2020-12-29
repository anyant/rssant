<?xml version="1.0" encoding="UTF-8"?>
<opml version="2.0">
    <head>
        <title>Feeds from RSSAnt [https://rss.anyant.com]</title>
    </head>
    <body>
    % for feed in feeds:
        <outline ${ feed['attrs'] }/>
    % endfor
    </body>
</opml>