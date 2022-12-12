html_start = \
'<h1 style="color: #1a1489;" data-darkreader-inline-color="">Sparc Download Statistics</h1> \
<p>This month, you have had downloads on the following datasets:</p> \
<table style="border-collapse: collapse; width: 100%;" border="1"> \
<tbody> \
<tr> \
<td style="width: 27.3912%;">Dataset ID</td> \
<td style="width: 22.6088%;">Version</td> \
<td style="width: 25%;">Origin</td> \
<td style="width: 25%;">Downloads</td> \
</tr>'



html_end = '\
</tbody> \
</table>'

def create_html_template(datasets_download_info):
    html_string = html_start
    for dataset in datasets_download_info:
        html_string += f'<tr> \
        <td style="width: 27.3912%;">{dataset["datasetId"]}</td> \
        <td style="width: 22.6088%;">{dataset["version"]}</td> \
        <td style="width: 25%;">{dataset["origin"]}</td> \
        <td style="width: 25%;">{dataset["downloads"]}</td> \
        </tr>'
    html_string += html_end
    return html_string

