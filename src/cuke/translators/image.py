from __future__ import absolute_import

import sys
from ..serializer import Translator, SerializationError, is_file_like
from ..serializers import Image

import numpy

_CV = False
try:
    cv2 = __import__('cv2')
    _CV = True
except:
    sys.stderr.write('Cannot find OpenCV, not creating Image serializer\n')
    pass

if _CV:
    class NumpyImageTranslator(Translator):
        @classmethod
        def known_wire_formats(cls,parent):
            return ['jpg','png','bmp','numpy']

        @classmethod
        def known_internal_formats(cls,parent):
            return ['numpy']

        @classmethod
        def extension_for_format(cls,format):
            return '.' + format

        @classmethod
        def is_binary(cls,parent,wire_format):
            return True

        @classmethod
        def deserialize(cls,parent,data,wire_format,internal_format):
            mat = cv2.imdecode(numpy.array(data), flags = cv2.CV_LOAD_IMAGE_UNCHANGED)
            if mat is None or len(mat) == 0:
                raise SerializationError()
            if internal_format == 'numpy':
                return mat
            else:
                _, mat = cv2.imencode(cls.extension_for_format(wire_format), mat)
                b = bytearray(mat.tostring())
                return b

        @classmethod
        def attempt_deserialize(cls,parent,data,internal_format):
            mat = cv2.imdecode(numpy.array(data), flags = cv2.CV_LOAD_IMAGE_UNCHANGED)
            if mat is None or len(mat) == 0:
                return None
            if internal_format == 'numpy':
                return mat
            else:
                wire_format = cls.known_wire_formats(parent)[0]
                _, mat = cv2.imencode(cls.extension_for_format(wire_format), mat)
                b = bytearray(mat.tostring())
                return b

        @classmethod
        def serialize(cls,parent,data,internal_format,wire_format):
            if is_file_like(data) or isinstance(data,str):
                #TODO: figure out format, convert if necessary
                return data
            data = numpy.array(data)
            _, mat = cv2.imencode(cls.extension_for_format(wire_format), data)
            b = bytearray(mat.tostring())
            return b
    Image.add_translator(NumpyImageTranslator)
