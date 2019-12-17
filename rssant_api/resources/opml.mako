<?xml version="1.0" encoding="UTF-8"?>
<opml version="1.0">
    <head>
        <title>Feeds from RSSAnt [https://rss.anyant.com]</title>
    </head>
    <body>
    % for feed in feeds:
        <outline text=${ feed['link'] } title=${ feed['title'] } type=${ feed['version'] } xmlUrl=${ feed['url'] }/>
    % endfor
    </body>
</opml>