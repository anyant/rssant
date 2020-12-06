<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="ie=edge">
    <title>Feed Analysis</title>
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
            font-family: monospace;
            font-size: 12px;
            border-collapse: collapse;
            margin-bottom: 48px;
            min-width: 720px;
        }

        table th{
            border: 1px solid #f3f3f3;
        }

        table tr.data:hover {
            background: #E7F4FE;
        }

        .domain {
            text-align: left;
        }

        .domain,
        .cell {
            border: 1px solid #f3f3f3;
        }

        .cell {
            text-align: right;
            min-width: 64px;
        }

        .cell .delta {
            text-align: center;
        }

        .cell.delta-pos .delta {
            color: #ffba00;
        }

        .cell.delta-neg .delta {
            color: #66bb6a;
        }

        .cell-wrapper {
            display: flex;
        }

        .cell .delta,
        .cell .value {
            flex: 1;
        }

        .cell.delta-zero .delta {
            visibility: hidden;
        }

        .cell.delta-zero .value {
            color: #353535;
        }

        .cell.value-zero .value {
            color: #f3f3f3;
        }

        .cell.delta-zero.value-zero .value {
            visibility: hidden;
        }
    </style>
</head>

<body>

<%
    def delta_class(value):
        if value > 0: return 'delta-pos'
        elif value < 0: return 'delta-neg'
        else: return 'delta-zero'

    def value_class(value):
        return 'value-zero' if value == 0 else ''
%>

<%def name="number_cell(row, key)">
    <td class="cell ${ value_class(row['base'][key]) } ${ delta_class(row['delta'][key]) }">
        <div class="cell-wrapper">
        <span class="value">${ row['base'][key] }</span>
        <span class="delta">${ '{:+}'.format(row['delta'][key]) }</span>
        </div>
    </td>
</%def>

    <table>
        <tr class="header">
            <th rowspan="2">domain</th>
            <th rowspan="2">total</th>
            <th rowspan="2">proxy</th>
            <th colspan="${ len(headers['response_status']) }">response_status</th>
            <th colspan="${ len(headers['freeze_level']) }">freeze_level</th>
        </tr>
        <tr class="header">
            % for name in headers['response_status']:
            <th>${ name }</th>
            % endfor
            % for name in headers['freeze_level']:
            <th>${ name }</th>
            % endfor
        </tr>
        % for row in records:
        <tr class="data">
            <td class="domain">${ row['domain'] }</td>
            ${ number_cell(row, 'total') }
            ${ number_cell(row, 'use_proxy') }
            % for name in headers['response_status']:
            ${ number_cell(row, 'response_status:' + name) }
            % endfor
            % for name in headers['freeze_level']:
            ${ number_cell(row, 'freeze_level:' + name) }
            % endfor
        </tr>
        % endfor
    </table>

</body>

</html>