from __future__ import absolute_import

import base64
import collections, numbers
import io
import re
import inspect

AUTOCONVERT_LIST = True
REQUIRED_BY_DEFAULT = False

SERIALIZER_NAME_BASE_PATTERN = r'(?P<name>(?:[a-zA-Z]\w*)(?:/(?:[a-zA-Z]\w*))?)'
SERIALIZER_PARAM_PATTERN = r'(?P<param>\(.+\))'
SERIALIZER_FORMAT_PATTERN = r'(?:\.(?P<format>[a-zA-Z]\w*))'
SERIALIZER_ARRAY_PATTERN = r'(?P<array>(?:\[(?:(?:\d+|\.\.\.)(?:,(?:\d+|\.\.\.))*)?\]))'

SERIALIZER_NAME_PATTERN = r'(?P<fullname>{name}{param}?{fmt}?){array}?'.format(
        name=SERIALIZER_NAME_BASE_PATTERN,
        param=SERIALIZER_PARAM_PATTERN,
        fmt=SERIALIZER_FORMAT_PATTERN,
        array=SERIALIZER_ARRAY_PATTERN)

class SerializationError(Exception):
    pass

class BinaryConversionError(SerializationError):
    pass

def binary_to_base64(data):
    """Convert data in binary format (bytearray or file-like) to base64-coded strings.
    Recurses into lists, tuples, and dicts."""
    if isinstance(data,list):
        data = [binary_to_base64(item) for item in data]
    elif isinstance(data,tuple):
        data = tuple([binary_to_base64(item) for item in data])
    elif isinstance(data,dict):
        for key, value in data.iteritems():
            if is_binary_data(value):
                if is_file_like(value):
                    value = value.read()
                data[key] = base64.b64encode(value)
            elif isinstance(value,(dict,list,tuple)):
                data[key] = binary_to_base64(value)
    elif is_binary_data(data):
        data = base64.b64encode(data)
    return data

def _check_format(format,known_formats):
    if format is None:
        return True
    if not isinstance(known_formats,(list,tuple)):
        return _check_format(format, [known_formats])
    for known_format in known_formats:
        if hasattr(known_format,'match') and known_format.match(format):
            return True
        elif format == known_format:
            return True
    return False

def deserialize(serializer,wire_data,wire_format=None):
    if wire_format and not serializer._call_can_deserialize(wire_format):
        raise SerializationError('Serializer %s does not accept format %s' % (serializer.get_name(), wire_format))
    if wire_data is None:
        if not serializer.is_nonesafe():
            if serializer.REQUIRED:
                raise SerializationError('Data is required!')
            return None
    if serializer._call_is_binary(wire_format):
        wire_data = _binary_convert(serializer, wire_format, wire_data, str2bin=True, file_ok=True)
    deserialized_data = serializer._call_deserialize(wire_data,wire_format)
    
    return deserialized_data

def serialize(serializer, internal_data, wire_format=None):
    if internal_data is None:
        if not serializer.is_nonesafe():
            if serializer.REQUIRED:
                raise SerializationError('Data is required!')
            return None
    if wire_format and not serializer._call_can_serialize(internal_data, wire_format):
        raise SerializationError('Serializer %s cannot serialize this data to format %s' % (serializer.get_name(), wire_format))
    if wire_format is None:
        wire_format = serializer._call_choose_wire_format(internal_data)
    serialized_data = serializer._call_serialize(internal_data, wire_format)
    if serializer._call_is_binary(wire_format):
        serialized_data = _binary_convert(serializer, wire_format, serialized_data, str2bin=False, file_ok=False)
    return serialized_data

class MetaSerializerBase(type):
    def __new__(cls, name, bases, dct):
        default_dct = {'__base_type__': None,
                      '__unformatted_type__': None, 
                      '__parent_type__': None, 
                      'INTERNAL_FORMAT': None, 
                      'NAMESPACE': '', 
                      'REQUIRED': REQUIRED_BY_DEFAULT}
        
        default_dct.update(dct)
        dct = default_dct

        return super(MetaSerializerBase, cls).__new__(cls, name, bases, dct)
    
    def is_nonesafe(self):
        return False
    
    def __getattr__(self,attr_name):
        return self.with_internal_format(attr_name)
    
    def with_internal_format(self,internal_format):
        dct = {}
        dct['__base_type__'] = self.get_base_type()
        dct['__unformatted_type__'] = self
        dct['__parent_type__'] = self
        dct['NAMESPACE'] = self.NAMESPACE
        
        if internal_format == 'required':
            subclass_name = self.__name__ + '__REQUIRED'
            dct['REQUIRED'] = True
            dct['INTERNAL_FORMAT'] = self.INTERNAL_FORMAT
        else:
            internal_format_str = internal_format if isinstance(internal_format,basestring) else 'COMPLEX'
            subclass_name = self.__name__ + '__FORMAT_' + internal_format_str
            dct['INTERNAL_FORMAT'] = internal_format
            dct['REQUIRED'] = self.REQUIRED
        
        dct['__name__'] = subclass_name
        
            
        @classmethod
        def get_name(cls):
            return self.get_name()
        dct['get_name'] = get_name
        return type(subclass_name,(self,),dct)
    
    @property
    def Field(self):
        """Required for use with Struct and MethodSignature.""" 
        return SerializerField(self)
    
    @property
    def List(self):
        """A serializer that handles lists of this serializer type."""
        return _get_list_serializer(self)
    
    def __getitem__(self, num_elem):
        return _get_list_serializer(self, num_elem)
    
class SerializerBase(object):
    """Base class for Serializer, Struct, and list- and callable-based
    serializers. Users should subclass Serializer.
    
    Internal formats may be specified when serializers are used for method
    inputs and outputs. There are two mechanisms for this: the class method
    with_internal_format(fmt), or as a shortcut, field-style access.
    As a convention, serializers may recognize a "raw" format that simply
    passes data through.
    Thus, a method may decorated @raas.method_input(MySerializer.raw) or
    equivalently @raas.method_input(MySerializer.with_internal_format('raw')
    
    For Structs, if a single format is specified, this is passed to the
    serializer for each field. If a list or dict is used, which can only
    be given using with_internal_format(), the internal formats for each
    field can be given separately. If a list is given, its length must be the
    number of fields in the struct."""
    __metaclass__   = MetaSerializerBase
    #__base_type__   = None
    #__parent_type__ = None
    
    #INTERNAL_FORMAT = None
    
    @classmethod
    def is_basic_type(cls):
        """Returns true if this is not a derived type (list, format, parameters, etc.)"""
        return cls.__base_type__ is None
    
    @classmethod
    def get_base_type(cls):
        """Get the base type for this class. If this serializer has an internal
        format or parameters set, this will return the class without them."""
        if cls.__base_type__ is None:
            return cls
        else:
            return cls.__base_type__
    
    @classmethod
    def get_unformatted_type(cls):
        """Get the base type for this class. If this serializer has an internal
        format set, this will return the class without the format, but will retain
        parameters."""
        if cls.__unformatted_type__ is None:
            return cls
        else:
            return cls.__unformatted_type__
    
    @classmethod
    def get_name(cls):
        """Returns the name of this serializer."""
        if cls.NAMESPACE:
            return cls.NAMESPACE + '/' + cls.__name__
        else:
            return cls.__name__
    
    @classmethod
    def choose_wire_format(cls,data,is_list=False):
        """Given the data, chooses the appropriate wire format for deserializing.
        
        Args:
            data: The data that will be serialized.
            is_list: If this argument is true, the data is a list, and the wire
                format should be chosen according to the contents of the list.
        """ 
        return None
    
    @classmethod
    def is_binary(cls,wire_format):
        """Returns true if the given wire format must be base64 encoded for sending
        in ASCII text."""
        return None
    
    @classmethod
    def can_deserialize(cls,wire_format):
        """Returns true if this serializer can deserialize the given wire format."""
        return NotImplemented
    
    @classmethod
    def can_serialize(cls,data,wire_format):
        """Returns true if this serializer can serialize into the given wire format."""
        return NotImplemented
    
    @classmethod
    def deserialize(cls,data,wire_format):
        """Deserialize the given data, assuming the given wire format."""
        return NotImplemented

    @classmethod
    def serialize(cls,data,wire_format):
        """Serialize the given data into the given wire format."""
        return NotImplemented
    
    #Internal methods 
    
    @classmethod
    def _call_choose_wire_format(cls,data,is_list=False):
        return cls.choose_wire_format(data,is_list=is_list)
    
    @classmethod
    def _call_is_binary(cls,wire_format):
        return cls.is_binary(wire_format)
    
    @classmethod
    def _call_can_deserialize(cls,wire_format):
        can = cls.can_deserialize(wire_format)
        if can is NotImplemented:
            raise NotImplementedError('can_deserialize() not implemented in %s' % str(cls))
        return can
    
    @classmethod
    def _call_can_serialize(cls,data,wire_format):
        can = cls.can_serialize(data, wire_format)
        if can is NotImplemented:
            raise NotImplementedError('can_serialize() not implemented in %s' % str(cls))
        return can
    
    @classmethod
    def _call_deserialize(cls,data,wire_format):
        data = cls.deserialize(data,wire_format)
        if data is NotImplemented:
            raise NotImplementedError('deserialize() not implemented in %s' % str(cls))
        return data

    @classmethod
    def _call_serialize(cls,data,wire_format):
        data = cls.serialize(data,wire_format)
        if data is NotImplemented:
            raise NotImplementedError('serialize() not implemented in %s' % str(cls))
        return data

class Translator(object):
    """Base class for translators that can be registered on a Serializer subclass,
    allowing the Serializer to extensibly process different wire and internal
    formats."""
    
    @classmethod
    def known_wire_formats(cls,parent):
        """Returns the wire formats this translator is capable of handling."""
        raise NotImplementedError('known_wire_formats() not implemented in %s' % str(cls))
    
    @classmethod
    def known_internal_formats(cls,parent):
        """Returns the internal formats this translator is capable of handling."""
        raise NotImplementedError('known_internal_formats() not implemented in %s' % str(cls))
    
    @classmethod
    def choose_wire_format(cls,parent,data,is_list=False):
        """Given the data, chooses the appropriate wire format for deserializing.
        
        By default, this chooses the first value given by known_wire_formats()
        
        Args:
            data: The data that will be serialized.
            is_list: If this argument is true, the data is a list, and the wire
                format should be chosen according to the contents of the list.
        """ 
        known_wire_formats = cls.known_wire_formats(parent)
        if known_wire_formats:
            return cls.known_wire_formats(parent)[0]
        else:
            return None
    
    @classmethod
    def is_binary(cls,parent,wire_format):
        """Returns true if the given wire format must be base64 encoded for sending
        in ASCII text."""
        return None
    
    @classmethod
    def can_deserialize(cls,parent,wire_format,internal_format):
        """Returns true if this serializer can deserialize the given wire format."""
        return _check_format(wire_format,cls.known_wire_formats(parent)) and _check_format(internal_format, cls.known_internal_formats(parent))
    
    @classmethod
    def can_serialize(cls,parent,data,internal_format,wire_format):
        """Returns true if this serializer can serialize into the given wire format."""
        return _check_format(wire_format,cls.known_wire_formats(parent)) and _check_format(internal_format, cls.known_internal_formats(parent))
    
    @classmethod
    def attempt_deserialize(cls,parent,data,internal_format):
        """Attempts to deserialize the given data in an unknown wire format
        into the given internal format. Returns None if the data cannot be
        deserialized by this Translator."""
        return None
    
    @classmethod
    def deserialize(cls,parent,data,wire_format,internal_format):
        """Deserializes the given data into the given internal format, assuming 
        the given wire format (which will not be None)."""
        raise NotImplementedError('deserialize() not implemented in %s' % str(cls))

    @classmethod
    def serialize(cls,parent,data,internal_format,wire_format):
        """Serializes the given data from the given internal format to the given
        wire format."""
        raise NotImplementedError('serialize() not implemented in %s' % str(cls))

def _parameter_str(param_list, param_dict, parens=True):
    base = ','.join(
                     [str(p) for p in param_list] + 
                     ['%s=%s' % (k,v) for k,v in param_dict.iteritems()])
    if parens:
        return '(' + base + ')'
    else:
        return base

class MetaSerializer(MetaSerializerBase):
    def __new__(cls, name, bases, dct):
        default_dct = {'TRANSLATORS': [], 
                       'PARAMETER_LIST': [], 
                       'PARAMETER_DICT': {}, 
                       '_PARAMETER_CHECK': None, 
                       'INTERNAL_FORMAT': None}
        
        default_dct.update(dct)
        dct = default_dct
        return super(MetaSerializer, cls).__new__(cls, name, bases, dct)
    
    def __call__(self,*args,**kwargs):
        return self._with_parameters(*args,**kwargs)
    
    def _with_parameters(self,*args,**kwargs):
        if self._PARAMETER_CHECK:
            args, kwargs = self._PARAMETER_CHECK(*args,**kwargs) or (args,kwargs)
        subclass_name = self.__name__
        subclass_name += '__PARAMS';
        if args:
            subclass_name += '_' + '_'.join([str(d) for d in args])
        if kwargs:
            subclass_name += '_' + '_'.join([str(k) + '_' + str(v) for k, v in kwargs.iteritems()])
        subclass_name = re.sub(r'[^a-zA-Z0-9]','_',subclass_name)
        
        dct = {}
        dct['__base_type__'] = self.get_base_type()
        dct['__parent_type__'] = self
        dct['PARAMETER_LIST'] = self.PARAMETER_LIST + list(args)
        dct['PARAMETER_DICT'] = self.PARAMETER_DICT.copy()
        dct['PARAMETER_DICT'].update(kwargs)
        
        dct['NAMESPACE'] = self.NAMESPACE
        
        dct['TRANSLATORS'] = self.TRANSLATORS
        dct['_PARAMETER_CHECK'] = self._PARAMETER_CHECK
        dct['INTERNAL_FORMAT'] = self.INTERNAL_FORMAT
        dct['REQUIRED'] = self.REQUIRED
        
        @classmethod
        def get_name(cls):
            return cls.get_base_type().get_name() + _parameter_str(cls.PARAMETER_LIST, cls.PARAMETER_DICT)
        dct['get_name'] = get_name
        return type(subclass_name,(self,),dct)
    
    def __getattr__(self,attr_name):
        dct = {}
        dct['__base_type__'] = self.get_base_type()
        dct['__unformatted_type__'] = self
        dct['__parent_type__'] = self
        dct['NAMESPACE'] = self.NAMESPACE
        
        dct['TRANSLATORS'] = self.TRANSLATORS
        dct['PARAMETER_LIST'] = self.PARAMETER_LIST
        dct['PARAMETER_DICT'] = self.PARAMETER_DICT
        dct['_PARAMETER_CHECK'] = self._PARAMETER_CHECK
            
        @classmethod
        def get_name(cls):
            return self.get_name()
        dct['get_name'] = get_name
        
        if attr_name == 'required':
            subclass_name = self.__name__ + '__REQUIRED'
            dct['REQUIRED'] = True
            dct['INTERNAL_FORMAT'] = self.INTERNAL_FORMAT
        else:
            dct['INTERNAL_FORMAT'] = attr_name
            dct['REQUIRED'] = self.REQUIRED
        
            if attr_name == 'raw':
                subclass_name = self.__name__ + '__RAW'
    
                @classmethod
                def can_deserialize(cls,wire_format):
                    return True
                dct['can_deserialize'] = can_deserialize
                
                @classmethod
                def deserialize(cls,data,wire_format):
                    return data
                dct['deserialize'] = deserialize
            else:
                internal_format_str = attr_name if isinstance(attr_name,basestring) else 'COMPLEX'
                subclass_name = self.__name__ + '__FORMAT_' + internal_format_str
        return type(subclass_name,(self,),dct)

class Serializer(SerializerBase):
    """Base class for all user-defined serializers.
    
    When implementing a serializer, there are two possible routes: overriding the
    methods of Serializer, or using Translators. For simple types, overriding is
    the best option. For complex types with multiple wire and internal formats,
    Translator provides an extensible mechanism to deal with it.
    
    User-defined serializers must be registered with the SerializerRegistry.
    """
    __metaclass__ = MetaSerializer
    
    @classmethod
    def add_translator(cls,translator):
        cls.TRANSLATORS.append(translator)
    
    @classmethod
    def translators(cls):
        if cls.__base_type__:
            return cls.__base_type__.TRANSLATORS
        else:
            return cls.TRANSLATORS
    
    @classmethod
    def known_wire_formats(cls):
        translators = cls.translators()
        if translators:
            formats = set()
            for trans in translators:
                formats.update(trans.known_wire_formats(cls))
            return list(formats)
        return NotImplemented
    
    @classmethod
    def known_internal_formats(cls):
        translators = cls.translators()
        if translators:
            formats = set()
            for trans in translators:
                formats.update(trans.known_internal_formats(cls))
            return list(formats)
        return []
        
    @classmethod
    def choose_wire_format(cls,data,is_list=False):
        translators = cls.translators()
        if not translators:
            known_formats = cls.known_wire_formats()
            if known_formats is not NotImplemented:
                if known_formats:
                    if not isinstance(known_formats[0],basestring):
                        raise NotImplementedError('choose_wire_format must be implemented in %s because known_formats returns regexp' % str(cls))
                    return known_formats[0]
                else:
                    return None
            raise NotImplementedError('choose_wire_format not implemented in %s' % str(cls))
        for trans in translators:
            if trans.can_serialize(cls,data,cls.INTERNAL_FORMAT,None):
                return trans.choose_wire_format(cls,data,is_list=is_list)
    
    #Internal methods 
    
    @classmethod
    def _call_choose_wire_format(cls,data,is_list=False):
        return cls.choose_wire_format(data,is_list=is_list)
    
    @classmethod
    def _call_is_binary(cls,wire_format):
        for trans in cls.translators():
            ret = trans.is_binary(cls,wire_format)
            if ret is not None:
                return ret
        return cls.is_binary(wire_format)
    
    @classmethod
    def _call_can_deserialize(cls,wire_format):
        translators = cls.translators()
        if not cls.INTERNAL_FORMAT and not translators:
            can = cls.can_deserialize(wire_format)
            if can is NotImplemented:
                known_formats = cls.known_wire_formats()
                return known_formats is NotImplemented or not known_formats or _check_format(wire_format, known_formats)
            else:
                return can
        
        if cls.INTERNAL_FORMAT == 'raw' or wire_format == cls.INTERNAL_FORMAT:
            return True
        for trans in translators:
            if trans.can_deserialize(cls, wire_format, cls.INTERNAL_FORMAT):
                return True
        can = cls.can_deserialize(wire_format)
        if can is NotImplemented:
            return False
        else:
            return can
    
    @classmethod
    def _call_can_serialize(cls,data,wire_format):
        if not cls.INTERNAL_FORMAT and not cls.translators():
            can = cls.can_serialize(data, wire_format)
            if can is NotImplemented:
                known_formats = cls.known_wire_formats()
                return known_formats is NotImplemented or not known_formats or _check_format(wire_format, known_formats)
            else:
                return can
        
        if wire_format == cls.INTERNAL_FORMAT:
            return True
        for trans in cls.TRANSLATORS:
            if trans.can_serialize(cls, data, cls.INTERNAL_FORMAT, wire_format):
                return True
        can = cls.can_serialize(data, wire_format)
        if can is NotImplemented:
            return False
        else:
            return can
    
    @classmethod
    def _call_deserialize(cls,data,wire_format):
        translators = cls.translators()
        if not translators:
            data = cls.deserialize(data,wire_format)
            if data is NotImplemented:
                raise NotImplementedError('deserialize() not implemented in %s' % str(cls))
            return data
        if wire_format:
            for trans in translators:
                if trans.can_deserialize(cls, wire_format, cls.INTERNAL_FORMAT):
                    internal_format = cls.INTERNAL_FORMAT
                    if not internal_format:
                        known_internal_formats = trans.known_internal_formats(cls)
                        if known_internal_formats:
                            internal_format = known_internal_formats[0]
                    return trans.deserialize(cls, data, wire_format, internal_format)
        for trans in translators:
            if cls.INTERNAL_FORMAT and not _check_format(cls.INTERNAL_FORMAT, trans.known_internal_formats(cls)):
                continue
            ret = trans.attempt_deserialize(cls, data, cls.INTERNAL_FORMAT)
            if ret is not None:
                return ret
        if cls.INTERNAL_FORMAT:
            raise SerializationError("%s could not deserialize data from wire format %s to internal format %s" % (cls.get_name(),wire_format,cls.INTERNAL_FORMAT))
        else:
            raise SerializationError("%s could not deserialize data from wire format %s" % (cls.get_name(),wire_format))
        

    @classmethod
    def _call_serialize(cls,data,wire_format):
        translators = cls.translators()
        if not translators:
            data = cls.serialize(data,wire_format)
            if data is NotImplemented:
                raise NotImplementedError('serialize() not implemented in %s' % str(cls))
            return data
        for trans in translators:
            if trans.can_serialize(cls,data,cls.INTERNAL_FORMAT,wire_format):
                if not wire_format:
                    wire_format = trans.choose_wire_format(cls,data)
                return trans.serialize(cls,data,cls.INTERNAL_FORMAT,wire_format)
        if cls.INTERNAL_FORMAT:
            raise SerializationError("%s could not serialize data from internal format %s to wire format %s" % (cls.get_name(),cls.INTERNAL_FORMAT,wire_format))
        else:
            raise SerializationError("%s could not serialize data to wire format %s" % (cls.get_name(),wire_format))
        
        

class SerializerField(object):
    """Instances of this class wrap Serializers to maintain ordering information
    in Structs and MethodSignatures. They do not need to be created directly;
    accessing the "Field" attribute of a Serializer class returns a SerializerField
    wrapping the Serializer."""
    instance_counter = 0
    
    def __init__(self,value):
        self.value = value
        self.counter = SerializerField.instance_counter
        SerializerField.instance_counter += 1

class _MetaListSerializer(MetaSerializerBase):
    def __call__(self,*args,**kwargs):
        if args:
            return _get_list_serializer(self.LIST_TYPE, args)
        if not kwargs:
            return self
        if len(kwargs) != 1 or 'ndims' not in kwargs:
            raise RuntimeError("Only valid kwarg for List is ndims of type int")
        if kwargs['ndims'] is None:
            return _get_list_serializer(self.LIST_TYPE, None)
        return _get_list_serializer(self.LIST_TYPE, tuple(Ellipsis for _ in xrange(int(kwargs['ndims']))))
    
    def __eq__(self, other):
        if inspect.isclass(other) and issubclass(other, _ListSerializer):
            return self.LIST_TYPE == other.LIST_TYPE and self.NUM_ELEM == other.NUM_ELEM
        elif isinstance(other,_ListSerializer):
            return self.__eq__(other.__class__)
        else:
            return False
    
    def __getitem__(self, item):
        raise RuntimeError("Cannot have lists of lists!")

class _ListSerializer(SerializerBase):
    __metaclass__ = _MetaListSerializer
    
    @classmethod
    def get_name(cls):
        return cls.LIST_TYPE.get_name() + '[' + ','.join(str(elem or '...')
                                                              for elem 
                                                              in (cls.NUM_ELEM or [])) + ']'
    
    @classmethod
    def choose_wire_format(cls,data,is_list=False):
        return cls.LIST_TYPE.choose_wire_format(data,is_list=True)
    
    @classmethod
    def is_binary(cls,wire_format):
        return cls.LIST_TYPE.is_binary(wire_format)
    
    @classmethod
    def can_deserialize(cls,wire_format):
        return cls.LIST_TYPE.can_deserialize(wire_format)
    
    @classmethod
    def can_serialize(cls,data,wire_format):
        data = data or []
        return all(cls.LIST_TYPE.can_serialize(d,wire_format) for d in data)
    
    @classmethod
    def _process_data(cls,function,data,format,num_elem=None,level=1,index=[0]):
        if num_elem is None:
            num_elem = cls.NUM_ELEM
        if not num_elem:
            return function(data, format)
        processed_data = []
        for idx, data_elem in enumerate(data or []):
            if format is None or isinstance(format,basestring):
                elem_format = format
            else:
                elem_format = format[idx]
            processed_data.append(cls._process_data(function,data_elem, elem_format, 
                                                     num_elem[1:], level+1,index + [idx]))
        if num_elem[0] is not None and len(processed_data) != num_elem[0]:
            raise SerializationError('%s requires exactly %d elements at index %s, got %d' % (
                                     cls.get_name(), num_elem[0], tuple(index), len(processed_data)))
        return processed_data
    
    @classmethod
    def deserialize(cls,data,wire_format):
        list_type = cls.LIST_TYPE
        if cls.INTERNAL_FORMAT == 'entries_required':
            list_type = list_type.required
        def func(data,wire_format):
            return deserialize(list_type,data,wire_format)
        deserialized_data = cls._process_data(func, data, wire_format)
        
        from .serializers import Float, Int
        if cls.INTERNAL_FORMAT == 'numpy' or (
                cls.INTERNAL_FORMAT != 'list' and issubclass(cls.LIST_TYPE,(Float,Int))):
            import numpy
            deserialized_data = numpy.array(deserialized_data)
        
        return deserialized_data

    @classmethod
    def serialize(cls,data,wire_format):
        list_type = cls.LIST_TYPE
        if cls.INTERNAL_FORMAT == 'entries_required':
            list_type = list_type.required
        def func(data,wire_format):
            return serialize(list_type,data,wire_format)
        return cls._process_data(func, data, wire_format, cls.NUM_ELEM, 1)

def _check_num_elem_entry(entry):
    if isinstance(entry, int):
        if entry <= 0:
            return None
        else:
            return entry
    elif entry is Ellipsis or entry is None:
        return None
    else:
        raise TypeError("%s is not a valid list of dimensions!" % entry)

def _get_list_serializer(list_type, num_elem=None, autoconvert=None):
    if autoconvert is None:
        autoconvert = AUTOCONVERT_LIST
    if not isinstance(num_elem,collections.Iterable):
        num_elem = (_check_num_elem_entry(num_elem),)
    else:
        num_elem = tuple(_check_num_elem_entry(entry) for entry in num_elem)
    
    from .serializers import Float, Vector, Matrix
    if issubclass(list_type,Float) and autoconvert:
        if len(num_elem) == 1:
            return Vector(num_elem)
        if len(num_elem) == 2:
            if num_elem[0] == 1 and num_elem[1] != 1:
                return Vector(num_elem[1]).rowmatrix
            elif num_elem[0] != 1 and num_elem[1] == 1:
                return Vector(num_elem[1]).colmatrix
            return Matrix(*num_elem)
    
    name = list_type.__name__ + '__LIST'
    dct = {'LIST_TYPE': list_type, 'NUM_ELEM': num_elem, '__base_type__': list_type.get_unformatted_type(), '__parent_type__': list_type}

    return type(name,(_ListSerializer,),dct)

def _get_list_serializer_field(value):
    if isinstance(value,list) and len(value) == 1:
        return _get_list_serializer_field(value[0])
    elif not isinstance(value,SerializerField):
        return None, None
    return _get_list_serializer(value.value), value.counter

def List(list_type, num_elem=None, ndims=None, autoconvert=None):
    if ndims is not None:
        num_elem = tuple(Ellipsis for _ in xrange(int(ndims)))
    return _get_list_serializer(list_type, num_elem, autoconvert=autoconvert)

class _MetaDictSerializer(MetaSerializerBase):
    def __eq__(self, other):
        if inspect.isclass(other) and issubclass(other, _DictSerializer):
            return self.KEY_TYPE == other.KEY_TYPE and self.VALUE_TYPE == other.VALUE_TYPE
        elif isinstance(other,_DictSerializer):
            return self.__eq__(other.__class__)
        else:
            return False

class _DictSerializer(SerializerBase):
    __metaclass__ = _MetaDictSerializer
    
    @classmethod
    def get_name(cls):
        return 'Dict(%s,%s)' % (cls.KEY_TYPE.get_name(), cls.VALUE_TYPE.get_name())
    
    @classmethod
    def choose_wire_format(cls,data,is_list=False):
        return tuple([cls.KEY_TYPE.choose_wire_format(data.keys(),is_list=True),
                cls.VALUE_TYPE.choose_wire_format(data.values(),is_list=True)])
    
    @classmethod
    def is_binary(cls,wire_format):
        key_format = wire_format[0] if wire_format else None
        value_format = wire_format[1] if wire_format else None
        return cls.KEY_TYPE.is_binary(key_format) or cls.VALUE_TYPE.is_binary(value_format)
    
    @classmethod
    def can_deserialize(cls,wire_format):
        return cls.KEY_TYPE.can_deserialize(wire_format[0]) or cls.VALUE_TYPE.can_deserialize(wire_format[1])
    
    @classmethod
    def can_serialize(cls,data,wire_format):
        data = data or {}
        return all([cls.KEY_TYPE.can_serialize(k,wire_format[0]) for k in data.iterkeys()]) and \
            all([cls.VALUE_TYPE.can_serialize(v,wire_format[1]) for v in data.itervalues()])
    
    @classmethod
    def deserialize(cls,data,wire_format):
        deserialized_data = {}
        if data is None: return deserialized_data
        for key_elem, value_elem in data.iteritems():
            key_wire_format, value_wire_format = wire_format or (None,None)
            if isinstance(key_wire_format,dict): 
                key_wire_format = key_wire_format.get(key_elem)
            key_data = cls.KEY_TYPE.deserialize(key_elem, key_wire_format)
            if isinstance(value_wire_format,dict):
                value_wire_format = value_wire_format.get(key_data,value_wire_format.get(key_elem))
            deserialized_data[key_data] = cls.VALUE_TYPE.deserialize(value_elem, value_wire_format)
        return deserialized_data

    @classmethod
    def serialize(cls,data,wire_format):
        serialized_data = {}
        if data is None: return serialized_data
        for key_elem, value_elem in data.iteritems():
            key_wire_format, value_wire_format = wire_format or (None,None)
            if isinstance(key_wire_format,dict): 
                key_wire_format = key_wire_format.get(key_elem)
            key_data = cls.KEY_TYPE.deserialize(key_elem, key_wire_format)
            if isinstance(value_wire_format,dict):
                value_wire_format = value_wire_format.get(key_elem)
            serialized_data[key_data] = cls.VALUE_TYPE.serialize(value_elem, value_wire_format)
        return serialized_data

def Dict(key_type,value_type):
    name = 'DICT__' + key_type.__name__ + '__' + value_type.__name__
    base_type = None
    dct = {'KEY_TYPE': key_type.get_unformatted_type(), 'VALUE_TYPE': value_type.get_unformatted_type(), '__base_type__': base_type}
    return type(name,(_DictSerializer,),dct)

def _get_dict_serializer_field(data):
    if isinstance(data,dict) and len(data) == 1:
        return _get_dict_serializer_field(data.items()[0])
    key = None
    value = None
    counter = None
    
    if isinstance(data[0],SerializerField):
        key = data[0].value
        counter = data[0].counter
    elif isinstance(data[0],Serializer):
        key = data[0]
    
    if isinstance(data[1],SerializerField):
        value = data[1].value
        counter = data[1].counter if counter is None else min(counter,data[1].counter)
    elif isinstance(data[1],Serializer):
        value = data[1]
    
    if key is None or value is None or counter is None:
        return None,None
    return Dict(key, value), counter

def _parse_dict_string(s):
    #TODO: error handling
    if not s.startswith('Dict('):
        return None,None
    s = s[5:-1]
    level = 0
    for idx, c in enumerate(s):
        if c == ',' and level == 0:
            break
        if c == '(': level += 1
        if c == ')': level -= 1
    else:
        return None,None
    return s[:idx], s[idx+1:]

class MetaStruct(MetaSerializerBase):
    def __new__(cls, name, bases, dct):

        new_dct = {}
        order = []
        for field_name, value in dct.iteritems():
            if isinstance(value,list) and len(value) == 1:
                serializer, counter = _get_list_serializer_field(value)
                if not serializer:
                    continue
                new_dct[field_name] = None
                new_dct[field_name + '__type'] = serializer
                order.append((counter,field_name,serializer))
            elif isinstance(value,dict) and len(value) == 1:
                serializer, counter = _get_dict_serializer_field(value)
                if not serializer:
                    continue
                new_dct[field_name] = None
                new_dct[field_name + '__type'] = serializer
                order.append((counter,field_name,serializer))
            elif isinstance(value,SerializerField):
                new_dct[field_name] = None
                serializer = value.value
                new_dct[field_name + '__type'] = serializer
                order.append((value.counter,field_name,serializer))
            else:
                new_dct[field_name] = value

        order = [v[1:3] for v in sorted(order,key=lambda v: v[0])]
        new_dct['_fields'] = order

        return super(MetaStruct, cls).__new__(cls, name, bases, new_dct)
    
    def __eq__(self, other):
        if inspect.isclass(other) and issubclass(other, Struct):
            return self.get_name() == other.get_name() and self._fields == other._fields
        elif isinstance(other,Struct):
            return self.__eq__(other.__class__)
        else:
            return False

class Struct(SerializerBase):
    """Superclass of serializer structs.
    
    A Struct is a serializer type that consists of an ordered list of fields,
    each with a corresponding serializer type (which may itself be a Struct).
    This allows the creation of complex, nested types that match data structures
    used by a given service.
    
    To define a struct, subclass Struct and list the fields of the struct with
    the value of each field being the corresponding serializer type with ".Field"
    appended (this helps Python remember the ordering of the fields). Thus, a 
    struct contain a field "id" with serializer type Int and a field "label"
    with serializer type String would be defined as follows:
    
    class LabeledId(Struct):
        id = Int.Field
        label = String.Field
    
    Structs can contain other structs. For example, a struct that contains a pose
    and the above struct could be defined as follows:
    
    class IdWithPose(Struct):
        pose = Pose.Field
        id = LabeledId.Field
    
    A field which is a list of some serializer type can be defined in two ways: by
    wrapping the serializer field type in a list (of length one), or by using the
    "List" property of the serializer. As an example:
    
    class StructWithLists(Struct):
        list1 = [Int.Field]
        list2 = Int.List.Field
    
    Structs cannot currently inherit from other Struct subclasses, as the 
    ordering of fields would be unclear.
    """
    __metaclass__ = MetaStruct
    
    def __init__(self,*args,**kwargs):
        if args and kwargs:
            raise TypeError("Can't use both args and kwargs creating %s" % self.get_name())
        if args and len(args) != len(self.get_fields()):
            raise TypeError("%s args must have exactly %d elements" % (self.get_name(), len(self.get_fields())))
        for idx, (field_name, field_type) in enumerate(self.get_fields()):
            
            arg_value = args[idx] if args else kwargs.get(field_name)
            if issubclass(field_type,Struct) and isinstance(arg_value,field_type):
                value = arg_value
            elif issubclass(field_type,Struct) and isinstance(arg_value,dict):
                value = instantiate_serializer(field_type,**arg_value)
            elif isinstance(arg_value,(list,tuple)):
                value = instantiate_serializer(field_type,*arg_value)
            elif isinstance(arg_value,dict):
                value = instantiate_serializer(field_type,**arg_value)
            else:
                value = instantiate_serializer(field_type,arg_value)
                
            setattr(self,field_name,value)
    
    @classmethod
    def get_field_names(cls):
        """Returns a list of the field names of this struct"""
        return [f[0] for f in cls._fields]
    
    @classmethod
    def get_field_type(cls,field_name):
        """Returns the serializer type of the given field"""
        return getattr(cls,field_name + '__type')
    
    @classmethod
    def get_fields(cls):
        """Get a list of (name,type) pairs of the fields"""
        return cls._fields
    
    @classmethod
    def choose_wire_format(cls,data,is_list=False):
        if data is None:
            return None
        if is_list:
            #TODO: something smarter
            data = data[0]
        format_dict = {}
        for field_name, field_type in cls.get_fields():
            format_dict[field_name] = field_type.choose_wire_format(data.get(field_name))
        return format_dict
    
    @classmethod
    def is_binary(cls,wire_format):
        for idx,(field_name, field_type) in enumerate(cls.get_fields()):
            
            if isinstance(wire_format,dict):
                field_wire_format = wire_format.get(field_name)
            elif isinstance(wire_format,list):
                field_wire_format = wire_format[idx]
            else:
                field_wire_format = wire_format
            
            if field_type.is_binary(field_wire_format):
                return True
        return False
    
    @classmethod
    def can_deserialize(cls,wire_format):
        for idx, (field_name, field_type) in enumerate(cls.get_fields()):
            
            if isinstance(wire_format,dict):
                field_wire_format = wire_format.get(field_name)
            elif isinstance(wire_format,list):
                field_wire_format = wire_format[idx]
            else:
                field_wire_format = wire_format
            
            if not field_type.can_deserialize(field_wire_format):
                return False
        return True
    
    @classmethod
    def can_serialize(cls,data,wire_format):
        for idx, (field_name, field_type) in enumerate(cls.get_fields()):
            
            field_data = data.get(field_name) if data else None
            
            if isinstance(wire_format,dict):
                field_wire_format = wire_format.get(field_name)
            elif isinstance(wire_format,list):
                field_wire_format = wire_format[idx]
            else:
                field_wire_format = wire_format
            
            if not field_type.can_serialize(field_data,field_wire_format):
                return False
        return True
    
    @classmethod
    def deserialize(cls,data,wire_format):
        deserialized_data = cls()
        for idx, (field_name, field_type) in enumerate(cls.get_fields()):
            
            if isinstance(wire_format,dict):
                field_wire_format = wire_format.get(field_name)
            elif isinstance(wire_format,list):
                field_wire_format = wire_format[idx]
            else:
                field_wire_format = wire_format
            
            if isinstance(data,dict):
                field_data = data[field_name]
                field_wire_format = data.get(field_name + '__fmt',field_wire_format)
            elif isinstance(data,list):
                field_data = data[idx]
            
            setattr(deserialized_data,field_name,deserialize(field_type, field_data, field_wire_format))
        return deserialized_data
    
    @classmethod
    def serialize(cls,data,wire_format):
        serialized_data = {}
        for idx, (field_name, field_type) in enumerate(cls.get_fields()):
            
            if data is None:
                serialized_data[field_name] = None
                continue
            
            if isinstance(data,Struct):
                field_data = getattr(data,field_name)
            else:
                field_data = data[field_name]
            
            if isinstance(wire_format,dict):
                field_wire_format = wire_format.get(field_name)
            elif isinstance(wire_format,list):
                field_wire_format = wire_format[idx]
            else:
                field_wire_format =  data.get(field_name + '__fmt',wire_format)
            
            serialized_data[field_name] = serialize(field_type, field_data, field_wire_format)
        return serialized_data
    
    def __contains__(self, key):
        return key in self.get_field_names()
  
    def __getitem__(self, key):
        if not key in self.get_field_names():
            raise KeyError("%s has no field named %s" % (self.get_name(),key))
        return getattr(self,key)
  
    def __setitem__(self, key, value):
        if not key in self.get_field_names():
            raise KeyError("%s has no field named %s" % (self.get_name(),key))
        setattr(self,key,value)

    def get(self, key, default=None):
        if not key in self.get_field_names():
            raise KeyError("%s has no field named %s" % (self.get_name(),key))
        return getattr(self,key)

    def has_key(self,key):
        return key in self.get_field_names()

    def items(self):
        return [(k, v) for k, v in self.iteritems()]
    
    def keys(self):
        return [k for k in self.iterkeys()]
    
    def values(self):
        return [v for v in self.itervalues()]
    
    def iteritems(self):
        for field_name in self.get_field_names():
            yield field_name, getattr(self,field_name)
        
    def iterkeys(self):
        for field_name in self.get_field_names():
            yield field_name
        
    def itervalues(self):
        for field_name in self.get_field_names():
            yield getattr(self,field_name)
    
    def todict(self):
        """Convert this struct instance into an OrderedDict"""
        return collections.OrderedDict(self.iteritems())
    
    def __str__(self):
        s = self.__class__.__name__
        s += '('
        strs = []
        for k, v in self.iteritems():
            strs.append(str(k) + '=' + str(v))
        s += ','.join(strs)
        s += ')'
        return s
    
    def __repr__(self):
        s = self.__class__.__name__
        s += '('
        strs = []
        for k, v in self.iteritems():
            strs.append(str(k) + '=' + str(v))
        s += ','.join(strs)
        s += ')'
        return s
    
    def __eq__(self, other):
        if inspect.isclass(other) and issubclass(other, Struct):
            return self.__class__ == other
        elif isinstance(other,Struct):
            if self.__class__ != other.__class__: return False
            return self.items() == other.items()
        else:
            return False

def _get_callable_serializer(func):
    class CallableSerializer(Serializer):
        @classmethod
        def name(cls):
            return func.__name__
        
        @classmethod
        def can_deserialize(cls,wire_format):
            return True
        
        @classmethod
        def can_serialize(cls,data,wire_format):
            return True
        
        @classmethod
        def deserialize(cls,data,wire_format):
            return func(data)
        
        @classmethod
        def serialize(cls,data,wire_format):
            return func(data)
    return CallableSerializer

def is_file_like(data):
    """Returns True if the input is file-like. In this context, that means the
    input is an instance of a subclass of file or io.IOBase, or is a StringIO
    instance."""
    return isinstance(data, (file, io.IOBase)) or (hasattr(data,'__class__') and getattr(data.__class__,'__name__',None) in ['StringIO','StringO'])

def is_binary_data(data):
    """Returns true if the input is a bytearray or is file-like."""
    return isinstance(data, bytearray) or is_file_like(data)

def _binary_convert(serializer_type,wire_format,data,str2bin,file_ok):
    if data is None: return None
    if not serializer_type.is_binary(wire_format):
        return data
    if issubclass(serializer_type,Struct):
        new_data = {}
        for idx, (k,v) in enumerate(data):
            subtype = serializer_type.get_field_type(k)
            if isinstance(wire_format,dict):
                subformat = wire_format.get(k)
            elif isinstance(wire_format,list):
                subformat = wire_format[idx]
            else:
                subformat = wire_format
            new_data[k] = _binary_convert(subtype,subformat,v,str2bin,file_ok)
        return new_data
    elif issubclass(serializer_type,_ListSerializer):
        new_data = []
        for idx, data_item in enumerate(data):
            if isinstance(wire_format,list):
                subformat = wire_format[idx]
            else:
                subformat = wire_format
            new_data.append(_binary_convert(serializer_type.LIST_TYPE,subformat,data_item, str2bin, file_ok))
        return new_data
    elif issubclass(serializer_type,_DictSerializer):
        new_data = {}
        for data_key, data_value in data.iteritems():
            key_wire_format, value_wire_format = wire_format or (None,None)
            if isinstance(key_wire_format,dict): 
                key_wire_format = key_wire_format.get(data_key)
            key_data = _binary_convert(serializer_type.KEY_TYPE,key_wire_format,data_value, str2bin, file_ok,recursed=False)
            if isinstance(value_wire_format,dict):
                value_wire_format = value_wire_format.get(key_data,value_wire_format.get(data_key))
            new_data[key_data] = _binary_convert(serializer_type.VALUE_TYPE,value_wire_format,data_value, str2bin, file_ok,recursed=False)
        return new_data
    
    if isinstance(data, bytearray):
        return data
    
    if isinstance(data, basestring):
        if str2bin:
            data = base64.b64decode(data)
        return bytearray(str(data))
    if is_file_like(data):
        if file_ok:
            return data
        return bytearray(data.read())
    
    raise BinaryConversionError()

def instantiate_serializer(serializer,*args,**kwargs):
    """Returns the given *args/**kwargs as an instance of the given serializer.
    For subclasses of Serializer, this is simply the data given. For Structs,
    this is an instance of the struct. For ListSerializers, this is the given
    *args with each value instantiated using the **kwargs (if any)."""
    if args and not kwargs and len(args) == 1 and args[0] is None:
        return None
    elif issubclass(serializer,Struct):
        return serializer(*args,**kwargs)
    elif issubclass(serializer, _ListSerializer):
        def func(data,wire_format):
            instantiate_serializer(serializer.__parent_type__,data,**kwargs)
        return serializer._process_data(func, args, None)
    elif issubclass(serializer, _DictSerializer):
        data = {}
        for key, value in kwargs.iteritems():
            data[instantiate_serializer(serializer.KEY_TYPE,key)] = instantiate_serializer(serializer.VALUE_TYPE,value)
        return data
    else:
        return args[0]

class SerializerRegistry(object):
    """The central registry of Serializers. The SerializerRegistry allows the
    client to find serializers used by services.
    """
    _builtins = {}
    _serializers = {}
    _finders = []
    
    @classmethod
    def _register_builtins(cls,*args):
        if len(args) == 1 and isinstance(args[0],collections.Sequence):
            args = args[0]
        for serializer in args:
            basic_type = serializer.get_base_type()
            basic_type_name = basic_type.get_name()
            
            if not cls._builtins.has_key(basic_type_name):
                cls._builtins[basic_type_name] = basic_type
            elif cls._builtins[basic_type_name] != basic_type:
                raise TypeError('A different type the name %s is already registered!' % basic_type_name)
    
    @classmethod
    def register(cls,*args):
        if len(args) == 1 and isinstance(args[0],collections.Sequence):
            args = args[0]
        for serializer in args:
            basic_type = serializer.get_base_type()
            basic_type_name = basic_type.get_name()
            
            if (cls._builtins.has_key(basic_type_name) or
                    issubclass(serializer, (_ListSerializer, _DictSerializer))):
                continue

            from .util import get_namespace
            basic_type.NAMESPACE = get_namespace(basic_type)
            basic_type_name = basic_type.get_name()
            
            if issubclass(basic_type, Struct):
                for _, field_type in basic_type.get_fields():
                    cls.register(field_type)
            
            if not cls._serializers.has_key(basic_type_name):
                cls._serializers[basic_type_name] = basic_type
            elif cls._serializers[basic_type_name] != basic_type:
                raise TypeError('A different type the name %s is already registered!' % basic_type_name)
    
    @classmethod
    def create_struct(cls, name, fields, namespace=None):
        if cls._serializers.has_key(name):
            struct = cls._serializers[name]
            if struct.get_fields() != fields:
                raise TypeError("Not equal!~")
            return struct
        if namespace is not None:
            name = re.sub(r'[^a-zA-Z0-9_]','_',name)
        elif '/' in name:
            idx = name.rfind('/')
            namespace = name[:idx]
            name = name[idx+1:]
        
        dct = {}
        if namespace is not None:
            dct['NAMESPACE'] = namespace
        for field_name, field_type in fields:
            dct[field_name] = field_type.Field
        struct = type(str(name),(Struct,),dct)
        cls.register(struct)
        return struct
    
    @classmethod
    def get_struct_instance(cls,struct_name,*args,**kwargs):
        struct = cls.get_serializer(struct_name)
        if not issubclass(struct,Struct):
            raise TypeError("%s is not a Struct!" % struct_name)
        else:
            return struct(*args,**kwargs)

    @classmethod
    def register_finder(cls,finder):
        cls._finders.append(finder)
    
    @classmethod
    def is_builtin_type(cls,type_or_type_name):
        if isinstance(type_or_type_name,basestring):
            match = re.match('^' + SERIALIZER_NAME_PATTERN + '$',type_or_type_name)
            if not match:
                raise KeyError('Invalid type name!')
            return cls._builtins.has_key(match.group('name'))
        else:
            for val in cls._builtins.itervalues():
                if issubclass(type_or_type_name,val):
                    return True
            return False
    
    @classmethod
    def has_serializer(cls,type_name,check_finders=True):
        if type_name.startswith('Dict('):
            key_type, value_type = _parse_dict_string(type_name)
            return cls.has_serializer(key_type, check_finders=check_finders) and cls.has_serializer(value_type, check_finders=check_finders)
        match = re.match('^' + SERIALIZER_NAME_PATTERN + '$',type_name)
        if not match:
            raise KeyError('Invalid type name: %s!' % type_name)
        groups = match.groupdict('')
        type_name = groups['name']
        known = cls._builtins.has_key(type_name) or cls._serializers.has_key(type_name)
        if not known and check_finders:
            for finder in cls._finders:
                ret = finder(type_name)
                if ret is not None:
                    cls._serializers[type_name] = ret
                    return True
        return known
    
    @classmethod
    def get_serializer(cls,type_name,go_easy=False):
        if type_name.startswith('Dict('):
            key_type, value_type = _parse_dict_string(type_name)
            return Dict(cls.get_serializer(key_type, go_easy=go_easy),cls.get_serializer(value_type, go_easy=go_easy))
        match = re.match('^' + SERIALIZER_NAME_PATTERN + '$',type_name)
        if not match:
            raise KeyError('Invalid type name: %s!' % type_name)
        groups = match.groupdict('')
        type_name = groups['name']
        if cls._builtins.has_key(type_name):
            serializer = cls._builtins[type_name]
        elif cls._serializers.has_key(type_name):
            serializer = cls._serializers[type_name]
        else:
            for finder in cls._finders:
                ret = finder(type_name)
                if ret is not None:
                    cls._serializers[type_name] = ret
                    return ret
            if go_easy:
                return None
            else:
                raise TypeError('Unknown type name %s!' % type_name)
        
        if groups['param']:
            serializer = eval('serializer' + groups['param'])
        if groups['format']:
            serializer = eval('serializer.' + groups['format'])
        
        array = groups['array']
        if array:
            dimstrs = array[1:-1].split(',')
            dims = []
            for dimstr in dimstrs:
                dim = Ellipsis if dimstr == '...' else int(dimstr)
                dims.append(dim)
            #TODO: autoconvert=False?
            serializer = _get_list_serializer(serializer,num_elem=dims)
        
        return serializer

import struct
class BinaryWithHeader(object):
    _TYPES = {int: 'i', float: 'd'}
    
    @classmethod
    def _get_types(cls):
        return set(cls._TYPES.keys() + [basestring])
    
    @classmethod
    def _pack_field(cls,field,value):
        if issubclass(field,basestring):
            value = str(value)
            l = len(value)
            return struct.pack(str(l+1) + 'p', value)
        else:
            return struct.pack(cls._TYPES[field],value)
    
    @classmethod
    def _unpack_field(cls,field,b):
        if issubclass(field,basestring):
            l = struct.unpack('b',str(b[0:1]))[0]
            if l == 0:
                return '', b[1:]
            else:
                value = ''.join(struct.unpack(str(l)+'c',str(b[1:l+1])))
                return value, b[l+1:]
        else:
            size = struct.calcsize(cls._TYPES[field])
            value = struct.unpack(cls._TYPES[field],str(b[:size]))[0]
            return value, b[size:]
    
    def __init__(self,*args,**kwargs):
        if isinstance(args[0], tuple):
            self.fields = []
            self.field_names = []
            for arg in args:
                if isinstance(arg[0],basestring):
                    self.fields.append(arg[1])
                    self.field_names.append(arg[0])
                else:
                    self.fields.append(arg[0])
                    self.field_names.append(arg[1])
        else:
            self.fields = args
            self.field_names = None
        
        for field in self.fields:
            if not issubclass(field,tuple(self._get_types())):
                raise TypeError('Unknown field type %s' % field)
    
    def pack(self,*args,**kwargs):
        b = bytearray()
        if kwargs:
            if not self.field_names:
                raise TypeError("Can't pack fields by name without field_names set!")
            elif len(args) != 1:
                raise ValueError("Payload argument is missing!")
            for field_name, field in zip(self.field_names,self.fields):
                value = kwargs.get(field_name,field())
                b.extend(self._pack_field(field,value))
        else:
            if len(args) != len(self.fields) + 1:
                raise ValueError('pack requires %d fields + binary data' % len(self.fields))
            for field, value in zip(self.fields,args[:-1]):
                b.extend(self._pack_field(field,value))
        
        b.extend(args[-1])
        
        return b
    
    def unpack(self,b):
        values = []
        for field in self.fields:
            value, b = self._unpack_field(field,b)
            values.append(value)
        
        if self.field_names:
            values = dict(zip(self.field_names,values))
        
        return values, b
