from __future__ import absolute_import

import re, collections, numbers, struct
import numpy as np
from ..serializer import Translator, SerializationError, is_file_like
from ..serializers import Rotation, Pose,Transform

from . import transformations
from raas.serializer import BinaryWithHeader

class NumpyRotationTranslator(Translator):
    @classmethod
    def known_wire_formats(cls,parent):
        return ['q.array',re.compile(r'matrix.float(16|32|64)')]
    
    @classmethod
    def known_internal_formats(cls,parent):
        return ['matrix','mat','q']
    
    @classmethod
    def can_serialize(cls, parent, data, internal_format, wire_format):
        if data is not None and not isinstance(data,(np.ndarray,list,tuple)):
            return False
        return super(NumpyRotationTranslator, cls).can_serialize(parent, data, internal_format, wire_format)
    
    @classmethod
    def is_binary(cls,parent,wire_format):
        return True
    
    @classmethod
    def deserialize(cls,parent,data,wire_format,internal_format):
        if 'stamped' in parent.PARAMETER_LIST:
            size = struct.calcsize('d')
            deserialized_data = {'stamp': None}
            stamp = struct.unpack('d',data[:size])[0]
            if stamp != -1:
                deserialized_data['stamp'] = stamp
            data = data[size:]
        q = None
        R = None
        if wire_format.startswith('matrix.float'):
            dtype = wire_format[wire_format.find('.')+1:]
            R = np.ones((4,4))
            R[0:3,0:3] = np.frombuffer(data,dtype=dtype).reshape((3,3))
            
        elif wire_format == 'q.array':
            l = len(data) / 4
            if l == 8:
                dtype = np.float64
            elif l == 4:
                dtype = np.float32
            elif l == 2:
                dtype = np.float16
            q = np.frombuffer(data,dtype=dtype)
        
        if internal_format in ['matrix','mat']:
            if R is None:
                R = transformations.quaternion_matrix(q)[0:3,0:3]
            value = R
        elif internal_format == 'q':
            if q is None:
                q = transformations.quaternion_from_matrix(R)[0:3,0:3]
            value = q
        
        if 'stamped' in parent.PARAMETER_LIST:
            deserialized_data['value'] = value
            return deserialized_data
        else:
            return value

    @classmethod
    def attempt_deserialize(cls,parent,data,internal_format):
        if 'stamped' in parent.PARAMETER_LIST:
            l = len(data) - struct.calcsize('d')
        else:
            l = len(data)
        dtypes = {'float64': 8, 'float32': 4, 'float16': 2}
        for dt, b in dtypes.iteritems():
            if l % b == 0 and l / b == 4:
                q = np.frombuffer(data, dtype=dt)
                if not any(np.isnan(q)) and np.allclose(np.linalg.norm(q),1):
                    return q
        for dt, b in dtypes.iteritems():
            if l % b == 0 and l / b == 9:
                R = np.frombuffer(data, dtype=dt).reshape((3,3))
                if not any(np.isnan(q)) \
                        and np.allclose(R.transpose(), R) \
                        and np.allclose(np.linalg.det(R),1):
                    return R
        return None

    @classmethod
    def serialize(cls,parent,data,internal_format,wire_format):
        if 'stamped' in parent.PARAMETER_LIST:
            stamp = float(-1)
            if isinstance(data,dict):
                stamp = data.get('stamp')
                data = data.get('value')
        T = _get_canonical_matrix(data)
        if wire_format == 'q.array':
            q = transformations.quaternion_from_matrix(T)
            value = q.tostring()
        if wire_format.startswith('matrix.'):
            dtype = np.dtype(wire_format[wire_format.find('.')+1:])
            T = T[0:3,0:3].astype(np.dtype(dtype))
            value = T.tostring()
        else:
            raise SerializationError('Unknown wire_format %s' % wire_format)
        
        if 'stamped' in parent.PARAMETER_LIST:
            value = struct.pack('d', stamp) + value
        
        return value
        
Rotation.add_translator(NumpyRotationTranslator)

class NumpyPoseTfTranslator(Translator):
    @classmethod
    def _get_bwh(cls,parent):
        bwh_fields = []
        if 'frame' in parent.PARAMETER_LIST:
            if issubclass(parent,Pose):
                bwh_fields.append((str,'frame'))
            else:
                bwh_fields.append((str,'from_frame'))
                bwh_fields.append((str,'to_frame'))
        if 'stamped' in parent.PARAMETER_LIST:
            bwh_fields.append((float,'stamp'))
        
        if bwh_fields:
            return BinaryWithHeader(*bwh_fields)
        else:
            return None

    @classmethod
    def known_wire_formats(cls,parent):
        return ['pq.array',re.compile(r'rowmajor\.float(16|32|64)')]
    
    @classmethod
    def known_internal_formats(cls,parent):
        return ['matrix','mat','pq','pr']
    
    @classmethod
    def can_serialize(cls, parent, data, internal_format, wire_format):
        if data is not None and not isinstance(data,(np.ndarray,list,tuple)):
            return False
        return super(NumpyPoseTfTranslator, cls).can_serialize(parent, data, internal_format, wire_format)
    
    @classmethod
    def is_binary(cls,parent,wire_format):
        return True
    
    @classmethod
    def deserialize(cls,parent,data,wire_format,internal_format):
        value_dict = None
        bwh = cls._get_bwh(parent)
        if bwh:
            value_dict, data = bwh.unpack(data)
            if value_dict.has_key('stamp') and value_dict['stamp'] == -1:
                value_dict['stamp'] = None
        
        pq = None
        T = None
        if wire_format.startswith('rowmajor.'):
            dtype = np.dtype(wire_format[wire_format.find('.')+1:])
            T = np.frombuffer(data,dtype=dtype).reshape((4,4))
        elif wire_format == 'pq.array':
            l = len(data) / 7
            if l == 8:
                dtype = np.float64
            elif l == 4:
                dtype = np.float32
            elif l == 2:
                dtype = np.float16
            pq = np.frombuffer(data,dtype=dtype)
        
        if internal_format in ['matrix','mat']:
            if T is None:
                T = np.eye(4)
                T[0:3,3] = pq[:3]
                T[0:3,0:3] = transformations.quaternion_matrix(pq[3:])[0:3,0:3]
            value = T
        elif internal_format == 'pq':
            if pq is None:
                p = T[0:3,3]
                q = transformations.quaternion_from_matrix(T)[0:3,0:3]
            else:
                p = pq[:3]
                q = pq[3:]
            value = (p,q)
        elif internal_format == 'pr':
            if pq is None:
                p = T[0:3,3]
                r = T[0:3,0:3]
            else:
                p = pq[:3]
                r = transformations.quaternion_matrix(pq[3:])[0:3,0:3]
            value = (p,r)
        
        if value_dict is not None:
            value_dict['value'] = value
            return value_dict
        else:
            return value
                
    
    @classmethod
    def attempt_deserialize(cls,parent,data,internal_format, try_bwh=True):
        if try_bwh:
            bwh = cls._get_bwh(parent)
            if bwh:
                try:
                    _, payload_data = bwh.unpack(data)
                    ret = cls.attempt_deserialize(parent, payload_data, internal_format, try_bwh=False)
                    if ret is None:
                        return cls.attempt_deserialize(parent, data, internal_format, try_bwh=False)
                    else:
                        return ret
                except:
                    pass
        
        l = len(data)
        
        if l % 7 == 0:
            wire_format = 'pq.array'
        elif l % 16 == 0:
            b = l/16
            if b == 8:
                wire_format = 'rowmajor.float64'
            elif b == 4:
                wire_format = 'rowmajor.float32'
            elif b == 2:
                wire_format = 'rowmajor.float16'
            else:
                return None
        else:
            return None
        
        return cls.deserialize(parent, data, wire_format, internal_format)

    @classmethod
    def serialize(cls,parent,data,internal_format,wire_format):
        bwh = cls._get_bwh(parent)
        bwh_input = {}
        if 'stamped' in parent.PARAMETER_LIST:
            stamp = float(-1)
            if isinstance(data,dict):
                stamp = data.get('stamp')
                data = data.get('value')
            bwh_input['stamp'] = stamp
        if 'frame' in parent.PARAMETER_LIST:
            if isinstance(data,dict):
                if issubclass(parent,Pose):
                    bwh_input['frame'] = data.get('frame')
                else:
                    bwh_input['from_frame'] = data.get('from_frame')
                    bwh_input['to_frame'] = data.get('to_frame')
                data = data.get('value')

        T = _get_canonical_matrix(data)
        if wire_format == 'pq.array':
            p = T[0:3,3]
            q = transformations.quaternion_from_matrix(T)
            value = np.hstack((p,q)).tostring()
        elif wire_format.startswith('rowmajor.'):
            dtype = np.dtype(wire_format[wire_format.find('.')+1:])
            T = T.astype(np.dtype(dtype))
            value = T.tostring()
        else:
            raise SerializationError('Unknown wire_format %s' % wire_format)
        
        if bwh:
            value = bwh.pack(value,**bwh_input)
        
        return value

Pose.add_translator(NumpyPoseTfTranslator)
Transform.add_translator(NumpyPoseTfTranslator)

def _get_canonical_matrix(data):
    T = np.identity(4, dtype=float)
    data_type, data = _get_data(data, default_4_to_quat=True)
    if data_type in ['tf','ps']:
        if isinstance(data,dict):
            if data.has_key('p'):
                T[0:3,3:4] = data['p']
            if data.has_key('r'):
                T[0:3,0:3] = data['r']
            elif data.has_key('q'):
                T[0:3,0:3] = transformations.quaternion_matrix(data['q'])[0:3,0:3]
        else:
            T = data
    elif data_type == 'p':
        T[0:3,3:4] = data
    elif data_type == 'r':
        T[0:3,0:3] = data
    elif data_type == 'q':
        T = transformations.quaternion_matrix(data)
    return T

def _get_axis_angle(val1,val2):
    if isinstance(val1,numbers.Number) and isinstance(val2,collections.Sequence) and len(val2) == 3:
        return (np.array(val2),val1)
    elif isinstance(val2,numbers.Number) and isinstance(val1,collections.Sequence) and len(val1) == 3:
        return (np.array(val1),val2)
    elif isinstance(val1,numbers.Number) and isinstance(val2,np.ndarray) and val2.size == 3:
        return (val2.reshape((3,)),val1)
    elif isinstance(val2,numbers.Number) and isinstance(val1,np.ndarray) and val1.size == 3:
        return (val1.reshape((3,)),val2)
    else:
        return None

def _get_type(value,default_4_to_quat=False,default_4_to_pt=False):
    #base types: tf, ps, p, q, r, u, s
    #sub types: d, v#, a#, l#, t
    
    def _is_rot_type(typ):
        base_type = typ.split('/')[0]
        return base_type == 'q' or base_type == 'r'
    
    def _is_pt_type(typ):
        base_type = typ.split('/')[0]
        return base_type == 'p'

    def _is_unknown_type(typ):
        base_type = typ.split('/')[0]
        return base_type == 'u'
    
    if value is None:
        return None
    if isinstance(value,numbers.Number):
        return 's'
    elif isinstance(value,dict):
        return _get_dict_data_type(value)
    elif isinstance(value,np.ndarray):
        if value.shape == (4,4):
            return 'tf/a44'
        elif value.shape == (3,3):
            return 'r/a33'
        elif value.size == 3:
            return 'p/v3'
        elif value.size == 4:
            if value.shape == (4,1):
                return 'p/v4'
            elif value.shape == (4,) or value.shape == (1,4):
                if value.flat[3] != 1:
                    return 'q/l4'
                elif abs(np.linalg.norm(value) - 1) > 1e-6:
                    return 'p/l4' 
                else:
                    if default_4_to_quat:
                        return 'q/a4'
                    elif default_4_to_pt:
                        return 'p/a4'
                    else:
                        return 'u/a4'
            else:
                return 'u/a' + str(value.shape[0]) + str(value.shape[1])
        else:
            if value.ndim == 1:
                return 'u/a' + str(len(value))
            else:
                return 'u/a' + str(value.shape[0]) + str(value.shape[1])
    elif isinstance(value,collections.Sequence):
        if len(value) == 16:
            return 'tf/l16'
        elif len(value) == 9:
            return 'r/l9'
        elif len(value) == 3:
            if all([isinstance(val,collections.Sequence) and len(val) == 3 for val in value]):
                return 'r/l33'
            elif all([isinstance(val,collections.Sequence) and len(val) == 1 for val in value]):
                return 'p/l31'
            elif all([isinstance(val,numbers.Number) for val in value]):
                return 'p/l3'
            else:
                return 'u/l3'
        elif len(value) == 4:
            if all([isinstance(val,collections.Sequence) and len(val) == 4 for val in value]):
                return 'tf/l44'
            elif all([isinstance(val,collections.Sequence) and len(val) == 1 for val in value]):
                return 'p/l41'
            else:
                if value[3] != 1:
                    return 'q/l4'
                elif abs(np.linalg.norm(value) - 1) > 1e-6:
                    return 'p/l4' 
                else:
                    if default_4_to_quat:
                        return 'q/l4'
                    elif default_4_to_pt:
                        return 'p/l4'
                    else:
                        return 'u/l4'
        elif len(value) == 1:
            if isinstance(value[0],collections.Sequence):
                if len(value[0]) == 3:
                    return 'p/l13'
                elif len(value[0]) == 4 and all([isinstance(val,numbers.Number) for val in value[0]]):
                    if value[0][3] != 1:
                        return 'q/l14'
                    elif abs(np.linalg.norm(value[0]) - 1) > 1e-6:
                        return 'p/l14' 
                    else:
                        if default_4_to_quat:
                            return 'q/l14'
                        elif default_4_to_pt:
                            return 'p/l14'
                        else:
                            return 'u/l14'
                else:
                    return 'u/l1'
            else:
                return 'u/l1'
        elif len(value) == 2:
            val1 = value[0]
            val2 = value[1]
            if isinstance(val1,numbers.Number) and isinstance(val2,collections.Sequence) and len(val2) == 3:
                return 'r/t/ax'
            elif isinstance(val2,numbers.Number) and isinstance(val1,collections.Sequence) and len(val1) == 3:
                return 'r/t/xa'
            elif isinstance(val1,numbers.Number) and isinstance(val2,np.ndarray) and val2.size == 3:
                return 'r/t/ax'
            elif isinstance(val2,numbers.Number) and isinstance(val1,np.ndarray) and val1.size == 3:
                return 'r/t/xa'
            else:
                type1 = _get_type(value[0])
                type2 = _get_type(value[1])
                if  _is_pt_type(type1) and _is_unknown_type(type2):
                    type2 = _get_type(value[1],default_4_to_quat=True)
                elif _is_pt_type(type2) and _is_unknown_type(type1):
                    type1 = _get_type(value[0],default_4_to_quat=True)
                if (_is_rot_type(type1) and _is_pt_type(type2)) or (_is_pt_type(type1) and _is_rot_type(type2)):
                    return 'tf/t/[' + type1 + ',' + type2 + ']'
                else:
                    return 'u/l2'
        else:
            return 'u/l' + str(len(value))
    else:
        return 'u'

def _get_dict_data_type(value):
    if all([value.has_key(key) for key in 'xyz']):
        if value.has_key('w'):
            return 'q/d/x'
        else:
            return 'p/d'
    elif all([value.has_key(key) for key in ['qx','qy','qz','qw']]):
        return 'q/d/qx'
    elif all([value.has_key(key) for key in ['yaw','pitch','roll']]):
        return 'r/tb/d'
    elif any([value.has_key(key) for key in ['position','pos','translation','trans','orientation','ori','rotation','rot','tb_angles','tb']]):
        is_pose = None
        if value.has_key('is_pose'):
            is_pose = value['is_pose']
        elif value.has_key('is_transform'):
            is_pose = not value['is_transform']
        elif value.has_key('is_tf'):
            is_pose = not value['is_tf']
        if is_pose is not None:
            if is_pose:
                return 'ps/d'
            else:
                return 'tf/d'
        else:
            return 'tf/d'
    elif value.has_key('pose'):
        return 'ps/d/o'
    elif any([value.has_key(key) for key in ['transform','tf']]):
        return 'tf/d/o'
    return 'u/d'

def _get_data(data,default_4_to_quat=False,default_4_to_pt=False):
    data_type = _get_type(data,default_4_to_quat=default_4_to_quat,default_4_to_pt=default_4_to_pt)
    
    base_type = data_type.split('/')[0]
    
    if base_type in ['s','u']:
        return (data_type,data)
    
    sub_types = data_type.split('/')[1:]
    
    if base_type in ['tf','ps']:
        return (base_type,_get_tf_data(data,sub_types,is_pose=base_type=='ps',default_4_to_quat=default_4_to_quat,default_4_to_pt=default_4_to_pt))
    elif base_type == 'p':
        return (base_type,_get_pt_data(data,sub_types))
    elif base_type == 'q':
        return (base_type,_get_quat_data(data,sub_types))
    elif base_type == 'r':
        return (base_type,_get_rot_data(data,sub_types))

def _get_tf_data(data,sub_types,is_pose,default_4_to_quat=False,default_4_to_pt=False):
    if sub_types[0] == 't':
        vals = {}
        for v in data:
            vals.__setitem__(*_get_data(v,default_4_to_quat=default_4_to_quat,default_4_to_pt=default_4_to_pt))
        return vals
    elif sub_types[0] == 'd':
        for key in ['pose','transform','tf']:
            if data.has_key(key):
                return _get_tf_data(data[key],sub_types,is_pose,default_4_to_quat=default_4_to_quat,default_4_to_pt=default_4_to_pt)
        vals = {}
        for key in ['position','pos','translation','trans']:
            if data.has_key(key):
                p_type, p_data = _get_data(data[key],default_4_to_quat=False,default_4_to_pt=True)
                if p_type.startswith('tf') or p_type.startswith('ps'):
                    p_type, p_data = _get_data(p_data['p'])
                vals[p_type] = p_data
                break
        for key in ['orientation','ori','rotation','rot']:
            if data.has_key(key):
                r_type, r_data = _get_data(data[key],default_4_to_quat=True,default_4_to_pt=False)
                if r_type.startswith('tf') or r_type.startswith('ps'):
                    r_type, r_data = _get_data(r_data['r'])
                vals[r_type] = r_data
                break
        return vals
    elif sub_types[0] in ['a44','l44']:
        return np.asarray(data)
    elif sub_types[0] == 'l16':
        return np.asarray(data).reshape((4,4))

def _get_pt_data(data,sub_types):
    if any([sub_types[0].startswith(t) for t in ['a','v','l']]):
        return np.asarray(data).flat[0:3].reshape((3,1))
    elif sub_types[0].startswith('d'):
        return np.array([data.get('x',0.),data.get('y',0.),data.get('z',0.)]).reshape((3,1))

def _get_quat_data(data,sub_types):
    if any([sub_types[0].startswith(t) for t in ['a','v','l']]):
        return np.asarray(data).flat[0:4]
    elif sub_types[0].startswith('d'):
        if sub_types[1] == 'x':
            return np.array([data['x'],data['y'],data['z'],data['w']])
        elif sub_types[1] == 'qx':
            return np.array([data['qx'],data['qy'],data['qz'],data['qw']])

def _get_rot_data(data,sub_types):
    if sub_types[0] in ['a33','l33']:
        return np.asarray(data)
    elif sub_types[0] == 'l9':
        return np.asarray(data).reshape((3,3))
    elif sub_types[0] == 't':
        if sub_types[1] in ['ax','xa']:
            axis, angle = _get_axis_angle(data[0], data[1])
            return transformations.quaternion_matrix(transformations.quaternion_about_axis(angle, axis))[0:3,0:3]