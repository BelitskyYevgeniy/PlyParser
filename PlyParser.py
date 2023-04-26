import re

class PlyFile:
    headers = ''
    elements = []

class PlyElement:
    properties = b''
    name = ''
    count = 0

class PlyParsingFailedException(Exception):
    def __init__(self, text):
        self.text = text

class PlyParser:
    @staticmethod
    def _is_delimeter(s):
        d_n = int.from_bytes(b'\n', "big")
        d_rn = int.from_bytes(b'\r\n', "big")
        return s == d_n or s == d_rn

    def parse(self, str):
        delimiter = int.from_bytes(b'\n', "big")
        length = len(str)

        file = PlyFile()

        i = 0
        line_start_i = i
        while length != i:
            if PlyParser._is_delimeter(str[i]):
                utf_str = str[line_start_i:i].decode('utf-8')
                match = re.findall(r'\w+', utf_str)
                if not match:
                    raise PlyParsingFailedException('Empty string was found in header')
                if match[0] == 'element':
                    element = PlyElement()

                    try:
                        element.name = match[1]
                        element.count = int(match[2])
                    except Exception as e:
                        raise PlyParsingFailedException('Wrong element declaration format')
                    file.elements.append(element)
                if match[0] == 'end_header':
                    break
                line_start_i = i + 1
            i += 1

        file.headers = str[0:i].decode('utf-8')

        for e in file.elements:
            i += 1
            start = i
            count = 0
            while length > i:
                if PlyParser._is_delimeter(str[i]):
                    count += 1
                    if count == e.count:
                        break
                i += 1
            if count != e.count:
                raise PlyParsingFailedException(f'Not full property string for \'{e.name}\' element')
            e.properties = str[start:i]
        return file
