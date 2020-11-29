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
            font-size: 12px;
            border-collapse: collapse;
            margin-bottom: 48px;
            min-width: 720px;
        }

        .domain {
            text-align: left;
        }

        .response-status-name {
            text-align: right;
        }

        .response-status-label {
            text-align: left;
            padding-left: 16px;
        }

        .domain,
        .cell {
            border: 1px solid #f3f3f3;
        }

        .cell {
            text-align: right;
            min-width: 48px;
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

        table td,
        table th {
            font-family: monospace;
            padding: 2px;
            border-collapse: collapse;
            word-break: keep-all;
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

<%def name="number_cell(stats, key)">
    <td class="cell ${ value_class(stats[key]) } ${ delta_class(stats[key+'_diff']) }">
        <div class="cell-wrapper">
        <span class="value ">${ stats[key] }</span>
        <span class="delta">${ stats[key+'_diff'] }</span>
        </div>
    </td>
</%def>

<%def name="count_cells(stats, key, names)">
    % for name in names:
    <td class="cell ${ value_class(stats[key][name]) } ${ delta_class(stats[key+'_diff'][name]) }">
        <div class="cell-wrapper">
        <span class="value">${ stats[key][name] }</span>
        <span class="delta">${ '{:+}'.format(stats[key+'_diff'][name]) }</span>
        </div>
    </td>
    % endfor
</%def>

    <h2>Overview</h2>
    <table>
        <tr>
            <th>domain</th>
            <th>total</th>
            <th>use_proxy</th>
            % for name in status_names:
            <th>${ name }</th>
            % endfor
        </tr>
        % for domain, stats in result:
        <tr>
            <td class="domain">${ domain }</td>
            ${ number_cell(stats, 'total') }
            ${ number_cell(stats, 'use_proxy') }
            ${ count_cells(stats, 'status', status_names) }
        </tr>
        % endfor
    </table>

    <h2>Response Status</h2>
    <table>
        <tr>
            <th>domain</th>
            % for name in response_status_names:
            <th>${ name }</th>
            % endfor
        </tr>
        % for domain, stats in result:
        <tr>
            <td class="domain">${ domain }</td>
            ${ count_cells(stats, 'response_status', response_status_names) }
        </tr>
        % endfor
    </table>

    <h2>Freeze Level</h2>
    <table>
        <tr>
            <th>domain</th>
            % for name in freeze_level_names:
            <th>${ name }</th>
            % endfor
        </tr>
        % for domain, stats in result:
        <tr>
            <td class="domain">${ domain }</td>
            ${ count_cells(stats, 'freeze_level', freeze_level_names) }
        </tr>
        % endfor
    </table>

    <h2>状态码表</h2>
    <table class="response-status">
    % for name, label in zip(response_status_names, response_status_labels):
        <tr>
        <td class="response-status-name">${ name }</td>
        <td class="response-status-label">${ label}</td>
        </tr>
    % endfor
    </table>

</body>

</html>