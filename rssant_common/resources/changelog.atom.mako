<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title type="text">${ title }</title>
  % if link:
  <link href="${ link }/changelog" rel="alternate"/>
  <link href="${ link }/changelog.atom" rel="self" type="application/atom+xml"/>
  <icon>${ link }/favicon.ico</icon>
  % endif
  <updated>${ format_date(updated) }</updated>
  % if link:
  <id>${ link }/changelog</id>
  % endif
  %for item in changelogs:
  <entry>
    <title type="text">${ item.version }: ${ item.title }</title>
    % if link:
    <link href="${ link }/changelog?version=${ item.version }"/>
    <id>${ link }/changelog?version=${ item.version }</id>
    % endif
    <updated>${ format_date(item.date) }</updated>
    <summary type="html">
    <![CDATA[${ item.html }]]>
    </summary>
  </entry>
  % endfor
</feed>