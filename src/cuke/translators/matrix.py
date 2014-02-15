from __future__ import absolute_import

import re, collections, numbers
import numpy as np
from ..serializer import Translator, SerializationError, is_file_like
from ..serializers import Matrix, IntegerMatrix

class NumpyMatrixTranslator(Translator):
    @classmethod
    def known_wire_formats(cls,parent):
        return ['q.array',re.compile(r'matrix.float(16|32|64)')]
    
    @classmethod
    def known_internal_formats(cls,parent):
        return ['numpy']
    
    @classmethod
    def is_binary(cls,parent,wire_format):
        return True
    
    @classmethod
    def deserialize(cls,parent,data,wire_format,internal_format):
        pass
        
    
    @classmethod
    def attempt_deserialize(cls,parent,data,internal_format):
        pass

    @classmethod
    def serialize(cls,parent,data,internal_format,wire_format):
        pass
        
Matrix.add_translator(NumpyMatrixTranslator)