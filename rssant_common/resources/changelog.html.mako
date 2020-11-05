<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="ie=edge">
    <title>${ title }</title>
    <link rel="alternate" type="application/atom+xml" href="${ link }/changelog.atom" />
    <style>
        ${ normalize_css }
    </style>
    <style>
        ${ github_markdown_css }

        /* Fix: font-weight: 600 not working in Android Chrome */
        .markdown-body strong {
            font-weight: bold;
        }

        .markdown-body h1,
        .markdown-body h2,
        .markdown-body h3,
        .markdown-body h4,
        .markdown-body h5,
        .markdown-body h6 {
            font-weight: bold;
        }

        .markdown-body dl dt,
        .markdown-body table th {
            font-weight: bold;
        }
    </style>
    <style>
        body {
            background: #f9f9f9;
            background-color: #f9f9f9;
        }
        .main {
            box-sizing: border-box;
            min-height: 100vh;
            min-width: 300px;
            max-width: 850px;
            margin: 0 auto;
            padding: 16px;
            background: #fefefe;
            background-color: #fefefe;
            line-height: 1.45;
        }
        .changelog-header {
            margin-top: 32px;
        }
        .changelog summary {
            font-weight: bold;
            outline: none;
        }
        .changelog-content {
            margin-top: 16px;
            margin-left: 24px;
            overflow: auto;
        }
    </style>
</head>

<body>
    <div class="main markdown-body">
        <h2>${ title }</h2>
        % for item in changelogs:
        <details class="changelog" ${ 'open' if loop.index == 0 else '' }>
            <summary class="changelog-header">
            ${ item.version } (${ item.date }) : ${ item.title }
            </summary>
            <div class="changelog-content">
            ${ item.html }
            </div>
        </details>
        % endfor
    </div>
</body>

</html>