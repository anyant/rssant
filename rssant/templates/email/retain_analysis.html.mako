<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="ie=edge">
    <title>Retain Analysis</title>
    <style>
        body {
            width: 100% !important;
            -webkit-text-size-adjust: 100%;
            -ms-text-size-adjust: 100%;
            margin: 0 !important;
            padding: 0 !important;
            color: #353535;
        }

        table {
            text-align: center;
            font-size: 12px;
            border-collapse: collapse;
            min-width: 720px;
        }

        .count {
            font-family: monospace;
        }

        table td,
        table th {
            padding: 0 2px;
            border-collapse: collapse;
            word-break: keep-all;
        }
    </style>
</head>

<body>
    <table>
        <tr>
            <th class="count">date</th>
            <th class="count">+</th>
            <th class="count">@</th>
            % for p in periods:
            <th class="count">${ p }</th>
            % endfor
            % for p in periods:
            <th class="count">${ p }</th>
            % endfor
        </tr>
        % for max_period, (dt, total, ratios, activated, activated_ratios) in zip(max_periods, rows):
        <tr>
            <td class="count">${ dt }</td>
            <td class="count">${ total }</td>
            <td class="count">${ activated }</td>
            % for period, value in zip(periods, ratios):
            <td style="background:rgba(102,187,106,${ value })">${ '{:.2f}'.format(value) if period <= max_period else '' }</td>
            % endfor
            % for period, value in zip(periods, activated_ratios):
            <td style="background:rgba(102,187,106,${ value })">${ '{:.2f}'.format(value) if period <= max_period else '' }</td>
            % endfor
        </tr>
        % endfor
    </table>
</body>

</html>