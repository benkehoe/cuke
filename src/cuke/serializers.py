from __future__ import absolute_import

from .serializer import *
from .serializer import _get_list_serializer
import sys
import time, datetime
import json

_NUMPY = False
try:
    import numpy
    _NUMPY = True
except:
    pass

from cStringIO import StringIO

from .util import iso8601

class PrimitiveTypeSerializer(Serializer):
    PRIMITIVE_TYPE = None
    
    @classmethod
    def is_nonesafe(cls):
        return cls.INTERNAL_FORMAT == 'default'
    
    @classmethod
    def known_wire_formats(cls):
        return []
    
    @classmethod
    def deserialize(cls,data,wire_format):
        if data is None and cls.INTERNAL_FORMAT == 'default':
            return cls.PRIMITIVE_TYPE()
        return cls.PRIMITIVE_TYPE(data)

    @classmethod
    def serialize(cls,data,wire_format):
        if data is None and cls.INTERNAL_FORMAT == 'default':
            return cls.PRIMITIVE_TYPE()
        return cls.PRIMITIVE_TYPE(data)

class Bool(PrimitiveTypeSerializer):
    PRIMITIVE_TYPE = bool    

class Int(PrimitiveTypeSerializer):
    PRIMITIVE_TYPE = int

class Float(PrimitiveTypeSerializer):
    PRIMITIVE_TYPE = float

class String(PrimitiveTypeSerializer):
    PRIMITIVE_TYPE = str

class Blob(Serializer):
    @classmethod
    def is_binary(cls):
        return True
    
    @classmethod
    def deserialize(cls,data,wire_format):
        return data

    @classmethod
    def serialize(cls,data,wire_format):
        return data

class JSON(Serializer):
    @classmethod
    def deserialize(cls,data,wire_format):
        if not data:
            return None
        return json.loads(data)

    @classmethod
    def serialize(cls,data,wire_format):
        return json.dumps(data)

class Timestamp(Serializer):
    @classmethod
    def known_wire_formats(cls):
        return ['iso','s','ms','ns']
    
    @classmethod
    def known_internal_formats(cls):
        return ['datetime','iso','s','ms','ns']
    
    @classmethod
    def deserialize(cls,data,wire_format):
        if wire_format is None:
            try:
                seconds = float(data)
                dt = datetime.datetime.fromtimestamp(seconds)
            except:
                try:
                    dt = iso8601.parse_date(data)
                    seconds = time.mktime(dt.timetuple())+1e-6*dt.microsecond
                except:
                    raise SerializationError('Unknown Timestamp data %s' % data)
        elif wire_format == 'iso':
            if cls.INTERNAL_FORMAT == 'iso':
                return data
            dt = iso8601.parse_date(data)
            seconds = time.mktime(dt.timetuple())+1e-6*dt.microsecond
        else:
            if wire_format == 's':
                seconds = float(data)
            elif wire_format == 'ms':
                seconds = float(data) / 1e3
            elif wire_format == 'ns':
                seconds = float(data) / 1e6
            else:
                raise SerializationError('Unknown Timestamp wire format %s' % wire_format)
            dt = datetime.datetime.fromtimestamp(seconds)
        
        if cls.INTERNAL_FORMAT == 'datetime':
            return dt
        elif cls.INTERNAL_FORMAT == 's':
            return seconds
        elif cls.INTERNAL_FORMAT == 'ms':
            return seconds * 1e3
        elif cls.INTERNAL_FORMAT == 'ns':
            return seconds * 1e6

    @classmethod
    def serialize(cls,data,wire_format):
        if cls.INTERNAL_FORMAT == 'iso' or isinstance(data,basestring):
            if wire_format == 'iso':
                return data
            dt = iso8601.parse_date(data)
            seconds = time.mktime(dt.timetuple())+1e-6*dt.microsecond
        else:
            if cls.INTERNAL_FORMAT == 's':
                seconds = float(data)
            elif cls.INTERNAL_FORMAT == 'ms':
                seconds = float(data) / 1e3
            elif cls.INTERNAL_FORMAT == 'ns':
                seconds = float(data) / 1e6
            dt = datetime.datetime.fromtimestamp(seconds)
        
        if wire_format == 'iso':
            return iso8601.print_date(dt)
        elif wire_format == 's':
            return seconds
        elif wire_format == 'ms':
            return seconds * 1e3
        elif wire_format == 'ns':
            return seconds * 1e6

class Duration(Serializer):
    @classmethod
    def known_wire_formats(cls):
        return ['s','ms','ns']
        
    @classmethod
    def known_internal_formats(cls):
        return ['s','ms','ns']
        
    @classmethod
    def deserialize(cls,data,wire_format):
        if wire_format == 's' or wire_format is None:
            seconds = float(data)
        elif wire_format == 'ms':
            seconds = float(data) / 1e3
        elif wire_format == 'ns':
            seconds = float(data) / 1e6
        else:
            raise SerializationError('Unknown Duration wire format %s' % wire_format)
        
        if cls.INTERNAL_FORMAT == 's':
            return seconds
        elif cls.INTERNAL_FORMAT == 'ms':
            return seconds * 1e3
        elif cls.INTERNAL_FORMAT == 'ns':
            return seconds * 1e6

    @classmethod
    def serialize(cls,data,wire_format):
        if cls.INTERNAL_FORMAT == 's' or cls.INTERNAL_FORMAT is None:
            seconds = float(data)
        elif cls.INTERNAL_FORMAT == 'ms':
            seconds = float(data) / 1e3
        elif cls.INTERNAL_FORMAT == 'ns':
            seconds = float(data) / 1e6
        else:
            raise SerializationError('Unknown Duration wire format %s' % wire_format)
        
        if wire_format == 's':
            return seconds
        elif wire_format == 'ms':
            return seconds * 1e3
        elif wire_format == 'ns':
            return seconds * 1e6

class Rotation(Serializer):
    pass

class Pose(Serializer):
    pass

class Transform(Serializer):
    pass

class Image(Serializer):
    pass

class Vector(Serializer):
    @classmethod
    def force_list(cls):
        dim = cls.PARAMETER_LIST[0] if cls.PARAMETER_LIST else None
        return _get_list_serializer(Float, dim, autoconvert=False)
    
    @classmethod
    def is_binary(cls, wire_format):
        if wire_format in ['numpy']:
            return True
        return False
    
    @classmethod
    def known_wire_formats(cls):
        return ['list','numpy']
    
    @classmethod
    def deserialize(cls, data, wire_format):
        if wire_format == 'numpy':
            mat = numpy.load(StringIO(data))
        else:
            mat = numpy.array(data)
        dim = cls.PARAMETER_LIST[0] if cls.PARAMETER_LIST else None
        if dim and mat.size != dim:
            raise SerializationError('This vector must have dimension %d, but it is %d!' % (dim, mat.size))
        
        if cls.INTERNAL_FORMAT in ['row','rowmatrix']:
            mat = mat.reshape((1,mat.size))
        elif cls.INTERNAL_FORMAT in ['col','column','colmatrix','columnmatrix']:
            mat = mat.reshape((mat.size,1))
        
        return mat
    
    @classmethod
    def serialize(cls, data, wire_format):
        dim = cls.PARAMETER_LIST[0] if cls.PARAMETER_LIST else None
        if dim and data.size != dim:
            raise SerializationError('This vector must have dimension %d, but it is %d!' % (dim, data.size))
        if wire_format == 'numpy':
            data = numpy.asarray(data,dtype=float).flatten()
            sio = StringIO()
            numpy.save(sio, data)
            data = sio.getvalue()
        else:
            if isinstance(data,numpy.ndarray):
                data = data.tolist()
        
        return data
    
    @classmethod
    def _PARAMETER_CHECK(cls,*args,**kwargs):
        if kwargs or len(args) != 1:
            raise ValueError("Vector parameter must be the dimension!")

class Matrix(Serializer):
    @classmethod
    def force_list(cls):
        dim = cls.PARAMETER_LIST if cls.PARAMETER_LIST else (None,None)
        return _get_list_serializer(Float, dim, autoconvert=False)
    
    @classmethod
    def is_binary(cls, wire_format):
        if wire_format in ['numpy']:
            return True
        return False
    
    @classmethod
    def known_wire_formats(cls):
        return ['list','numpy']
    
    @classmethod
    def deserialize(cls, data, wire_format):
        if wire_format == 'numpy':
            mat = numpy.load(StringIO(data))
        else:
            mat = numpy.array(data)
        rows,cols = cls.PARAMETER_LIST if cls.PARAMETER_LIST else (None,None)
        if rows and mat.shape[0] != rows:
            raise SerializationError('This matrix must have %d rows, but it has shape %s!' % (rows, mat.shape))
        if cols and mat.shape[1] != cols:
            raise SerializationError('This matrix must have %d columns, but it has shape %s!' % (cols, mat.shape))
        return mat
    
    @classmethod
    def serialize(cls, data, wire_format):
        rows,cols = cls.PARAMETER_LIST if cls.PARAMETER_LIST else (None,None)
        if wire_format == 'numpy':
            data = numpy.asarray(data,dtype=float)
            
            if rows and data.shape[0] != rows:
                raise SerializationError('This matrix must have %d rows, but it has shape %s!' % (rows, data.shape))
            if cols and data.shape[1] != cols:
                raise SerializationError('This matrix must have %d columns, but it has shape %s!' % (cols, data.shape))
            
            sio = StringIO()
            numpy.save(sio, data)
            data = sio.getvalue()
        else:
            checked = False
            if isinstance(data,numpy.ndarray):
                checked = True
                if rows and data.shape[0] != rows:
                    raise SerializationError('This matrix must have %d rows, but it has shape %s!' % (rows, data.shape))
                if cols and data.shape[1] != cols:
                    raise SerializationError('This matrix must have %d columns, but it has shape %s!' % (cols, data.shape))
                data = data.tolist()
            
            if not checked:
                if len(data) != rows:
                    raise SerializationError('This matrix must have %d rows, but it has %d!' % (rows, len(data)))
                for idx, value in enumerate(data):
                    if len(value) != cols:
                        raise SerializationError('This matrix must have %d cols, but row %d has %d!' % (cols, idx, len(data)))
        
        return data
    
    @classmethod
    def _PARAMETER_CHECK(cls,*args,**kwargs):
        if kwargs or len(args) != 2:
            raise ValueError("Matrix parameters must be rows and columns!")

class PointCloud(Serializer):

    @classmethod
    def known_formats(cls):
        return []
    
    @classmethod
    def is_binary(cls,wire_format):
        return True
        return format == 'pcd.binary' or format == 'raw'
    
    @classmethod
    def deserialize(cls,data,wire_format):
        if cls.deserialized_format in ['raw', wire_format]:
            return data
        else:
            raise NotImplementedError()

    @classmethod
    def serialize(cls,data,wire_format):
        if is_file_like(data):
            #TODO: figure out format, convert if necessary
            return data
        else:
            raise NotImplementedError()

if not SerializerRegistry._builtins:
    SerializerRegistry._register_builtins(Bool,Int,Float,String,Blob,Timestamp,Duration,Pose,Transform,Vector,Matrix,Image,PointCloud)

from . import translators
