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
            max-width: 640px;
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
        .changelog-content > p {
            padding-left: 24px;
        }
        .changelog-content > ul,
        .changelog-content > ol {
            padding-left: 46px;
        }
    </style>
</head>

<body>
    <div class="main">
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