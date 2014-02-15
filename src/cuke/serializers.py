from __future__ import absolute_import

from .serializer import *
import numpy, sys
import time, datetime
import json

from cStringIO import StringIO

from .util import iso8601

class PrimitiveTypeSerializer(Serializer):
    PRIMITIVE_TYPE = None
    
    @classmethod
    def known_wire_formats(cls):
        return []
    
    @classmethod
    def deserialize(cls,data,wire_format):
        if data is None:
            if cls.INTERNAL_FORMAT == 'default':
                return cls.PRIMITIVE_TYPE()
            return None
        else:
            return cls.PRIMITIVE_TYPE(data)

    @classmethod
    def serialize(cls,data,wire_format):
        if data is None:
            if cls.INTERNAL_FORMAT == 'default':
                return cls.PRIMITIVE_TYPE()
            return None
        else:
            return cls.PRIMITIVE_TYPE(data)

class Bool(PrimitiveTypeSerializer):
    PRIMITIVE_TYPE = bool    

class Int(PrimitiveTypeSerializer):
    PRIMITIVE_TYPE = int

class Float(PrimitiveTypeSerializer):
    PRIMITIVE_TYPE = float

class String(PrimitiveTypeSerializer):
    PRIMITIVE_TYPE = str

class NDArray(Serializer):
    @classmethod
    def _check(cls,mat,check_dtype):
        shape = cls.PARAMETER_LIST
        if shape:
            if len(shape) != mat.ndim:
                raise SerializationError('Array must be %d-dimensional, got %d' % (mat.ndim, len(shape)))
            for idx, req_size, mat_size in zip(xrange(len(shape)),shape,mat.shape):
                if req_size and mat_size != req_size:
                    raise SerializationError('Array dimension %d must be size %d, got %d' % (idx,req_size,mat_size))
        ndim = cls.PARAMETER_DICT.get('ndim')
        if ndim and mat.ndim != ndim:
            raise SerializationError('Array must be %d-dimensional, got %d' % (mat.ndim, ndim))
        dtype = cls.PARAMETER_DICT.get('dtype')
        if dtype and check_dtype:
            mat = numpy.asarray(mat, dtype=dtype)
        return mat
    
    @classmethod
    def is_binary(cls, wire_format):
        return True
    
    @classmethod
    def known_wire_formats(cls):
        return ['numpy']
    
    @classmethod
    def deserialize(cls, data, wire_format):
        mat = numpy.load(StringIO(data))
        mat = cls._check(mat, check_dtype=True)
        return mat
    
    @classmethod
    def serialize(cls, data, wire_format):
        data = cls._check(data, check_dtype=False)
        sio = StringIO()
        numpy.save(sio, data)
        return sio.getvalue()
    
    @classmethod
    def _PARAMETER_CHECK(cls,*args,**kwargs):
        ndim = kwargs.get('ndim')
        if args and ndim and len(args) != ndim:
            raise ValueError("Array shape %s and dimension %d are inconsistent!" % (args,ndim))

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
    def is_binary(cls, wire_format):
        return True
    
    @classmethod
    def known_wire_formats(cls):
        return ['numpy']
    
    @classmethod
    def deserialize(cls, data, wire_format):
        mat = numpy.load(StringIO(data))
        dim = cls.PARAMETER_LIST or None
        if dim and mat.size != dim:
            raise SerializationError('This vector must have dimension %d, but it is %d!' % (dim, mat.size))
        return mat
    
    @classmethod
    def serialize(cls, data, wire_format):
        dim = cls.PARAMETER_LIST or None
        if dim and data.size != dim:
            raise SerializationError('This vector must have dimension %d, but it is %d!' % (dim, data.size))
        sio = StringIO()
        numpy.save(sio, data)
        return sio.getvalue()
    
    @classmethod
    def _PARAMETER_CHECK(cls,*args,**kwargs):
        if kwargs or len(args) != 1:
            raise ValueError("Vector parameter must be the dimension!")

class Matrix(Serializer):
    @classmethod
    def is_binary(cls, wire_format):
        return True
    
    @classmethod
    def known_wire_formats(cls):
        return ['numpy']
    
    @classmethod
    def deserialize(cls, data, wire_format):
        mat = numpy.load(StringIO(data))
        rows, cols = cls.PARAMETER_LIST or (None,None)
        if rows and mat.shape[0] != rows:
            raise SerializationError('This matrix must have %d rows, but it has shape %s!' % (rows, mat.shape))
        if cols and mat.shape[1] != cols:
            raise SerializationError('This matrix must have %d columns, but it has shape %s!' % (cols, mat.shape))
        return mat
    
    @classmethod
    def serialize(cls, data, wire_format):
        rows, cols = cls.PARAMETER_LIST or (None,None)
        if rows and data.shape[0] != rows:
            raise SerializationError('This matrix must have %d rows, but it has shape %s!' % (rows, data.shape))
        if cols and data.shape[1] != cols:
            raise SerializationError('This matrix must have %d columns, but it has shape %s!' % (cols, data.shape))
        sio = StringIO()
        numpy.save(sio, data)
        return sio.getvalue()
    
    @classmethod
    def _PARAMETER_CHECK(cls,*args,**kwargs):
        if kwargs or len(args) != 2:
            raise ValueError("Matrix parameters must be rows and columns!")

class IntegerMatrix(Serializer):
    @classmethod
    def is_binary(cls, wire_format):
        return True
    
    @classmethod
    def known_wire_formats(cls):
        return ['numpy']
    
    @classmethod
    def deserialize(cls, data, wire_format):
        mat = numpy.load(StringIO(data))
        rows, cols = cls.PARAMETER_LIST or (None,None)
        if rows and mat.shape[0] != rows:
            raise SerializationError('This matrix must have %d rows, but it has shape %s!' % (rows, mat.shape))
        if cols and mat.shape[1] != cols:
            raise SerializationError('This matrix must have %d columns, but it has shape %s!' % (cols, mat.shape))
        if not re.match(r'u?int',str(mat.dtype)):
            raise SerializationError('IntegerMatrix given non-integer data!')
        return mat
    
    @classmethod
    def serialize(cls, data, wire_format):
        if not re.match(r'u?int',str(data.dtype)):
            raise SerializationError('IntegerMatrix given non-integer data!')
        rows, cols = cls.PARAMETER_LIST or (None,None)
        if rows and data.shape[0] != rows:
            raise SerializationError('This matrix must have %d rows, but it has shape %s!' % (rows, data.shape))
        if cols and data.shape[1] != cols:
            raise SerializationError('This matrix must have %d columns, but it has shape %s!' % (cols, data.shape))
        sio = StringIO()
        numpy.save(sio, data)
        return sio.getvalue()
    
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
    SerializerRegistry._register_builtins(Bool,Int,Float,String,NDArray,Blob,Timestamp,Duration,Pose,Transform,Vector,Matrix,IntegerMatrix,Image,PointCloud)

from . import translators
