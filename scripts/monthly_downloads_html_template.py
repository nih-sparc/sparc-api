html_start = \
'<h1 style="color: #1a1489;" data-darkreader-inline-color="">SPARC Download Statistics</h1> \
<p>This month, you have had downloads of the following datasets:</p> \
<table style="border-collapse: collapse; width: 100%;" border="1"> \
<tbody> \
<tr> \
<td style="width: 60%;">Dataset Name</td> \
<td style="width: 6%;">ID</td> \
<td style="width: 10%;">Version</td> \
<td style="width: 10%;">Origin</td> \
<td style="width: 14%;">Downloads</td> \
</tr>'



html_end = '\
</tbody> \
</table>'

def create_html_template(datasets_download_info):
    html_string = html_start
    for dataset in datasets_download_info:
        html_string += f'<tr> \
        <td style="width: 60%;">{dataset["name"]}</td> \
        <td style="width: 6%;">{dataset["datasetId"]}</td> \
        <td style="width: 10%;">{dataset["version"]}</td> \
        <td style="width: 10%;">{dataset["origin"]}</td> \
        <td style="width: 14%;">{dataset["downloads"]}</td> \
        </tr>'
    html_string += html_end
    return html_string

