import re

from xml.etree import ElementTree


XMP_NS = {'xmp': 'http://ns.adobe.com/xap/1.0/', 'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', 'x': 'adobe:ns:meta/'}
XMP_CORE_REGEXP = r"^XMP Core ([\d]+\.[\d]+\.[\d])$"


def _process_5_5_0(xml):
    xmp_info = {}
    pixel_width_element = xml.find('.//rdf:Description[@xmp:PixelWidth]', XMP_NS)
    if pixel_width_element is not None:
        xmp_info['pixel_width'] = pixel_width_element.attrib[f'{{{XMP_NS["xmp"]}}}PixelWidth']
    pixel_height_element = xml.find('.//rdf:Description[@xmp:PixelHeight]', XMP_NS)
    if pixel_height_element is not None:
        xmp_info['pixel_height'] = pixel_height_element.attrib[f'{{{XMP_NS["xmp"]}}}PixelHeight']
    z_spacing_element = xml.find('.//rdf:Description[@xmp:SizeZ]', XMP_NS)
    if z_spacing_element is not None:
        xmp_info['z_spacing'] = z_spacing_element.attrib[f'{{{XMP_NS["xmp"]}}}SizeZ']

    if not xmp_info:
        # As a backup, try processing the xml as version 5.6.0.
        xmp_info = _process_5_6_0(xml)

    return xmp_info


def _process_5_6_0(xml):
    xmp_info = {}
    pixel_width_element = xml.find('.//rdf:li[@xmp:PixelWidth]', XMP_NS)
    if pixel_width_element is not None:
        xmp_info['pixel_width'] = pixel_width_element.attrib[f'{{{XMP_NS["xmp"]}}}PixelWidth']
    pixel_height_element = xml.find('.//rdf:li[@xmp:PixelHeight]', XMP_NS)
    if pixel_height_element is not None:
        xmp_info['pixel_height'] = pixel_height_element.attrib[f'{{{XMP_NS["xmp"]}}}PixelHeight']
    z_spacing_element = xml.find('.//rdf:li[@xmp:SpacingZ]', XMP_NS)
    if z_spacing_element is not None:
        xmp_info['z_spacing'] = z_spacing_element.attrib[f'{{{XMP_NS["xmp"]}}}SpacingZ']

    element = xml.find('.//rdf:li[@xmp:Description]', XMP_NS)
    if element is not None:
        image_description = element.attrib[f'{{{XMP_NS["xmp"]}}}Description']
        image_description_mbf_map = image_description[image_description.find('<mbf_map>') + len('<mbf_map>'):]
        mbf_map = image_description_mbf_map[:image_description_mbf_map.find('</mbf_map>')]

        # Get the modality, expect it to be the same for all channels.
        matched = re.findall(r'Channel:0:[0-9]+:AcquisitionMode\?([^?]+)\?', mbf_map)
        matched = list(set(matched))
        if len(matched) == 1:
            xmp_info['modality'] = matched[0]
        else:
            xmp_info['modality'] = 'RGB'

        # Get the channel colours and names.
        matched_colour = re.findall(r'Channel:0:([0-9]+):Color\?([^?]+)\?', mbf_map)
        matched_name = re.findall(r'Channel:0:([0-9]+):Name\?([^?]+)\?', mbf_map)
        if len(matched_colour) == len(matched_name):
            xmp_info['channel_colours'] = [{}] * len(matched_colour)
            for name in matched_name:
                index = int(name[0])
                colour_list = [colour[1] for colour in matched_colour if colour[0] == name[0]]
                colour = int(colour_list[0])
                name = name[1]
                xmp_info['channel_colours'][index] = {'colour': '#{0:06X}'.format((colour >> 8) & 0xffffff), 'label': name}

        # Determine if image is 3D or 2D.
        matched = re.findall(r'SizeZ\?([^?]+)\?', mbf_map)
        if len(matched) == 1:
            xmp_info['three_d'] = int(matched[0]) > 1

    return xmp_info


def process_results(data):
    xml = ElementTree.fromstring(data)
    xmp_version_string = xml.get(f"{{{XMP_NS['x']}}}xmptk")

    match = re.match(XMP_CORE_REGEXP, xmp_version_string)
    if match is not None:
        version = match.group(1)
        handling_function_name = f"_process_{version.replace('.', '_')}"
        handling_function = globals().get(handling_function_name, None)
        if handling_function is not None:
            return handling_function(xml)

        raise NotImplementedError(f"Not able to handle XMP core meta data for version: '{version}'")

    raise AttributeError(f"Not able to match version from: '{xmp_version_string}'")
