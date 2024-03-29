#
# Autogenerated by Thrift Compiler (0.8.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TException
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class Iface:
  def get_image(self, id):
    """
    Parameters:
     - id
    """
    pass

  def get_images_since(self, image_id, timestamp, offset, limit):
    """
    Parameters:
     - image_id
     - timestamp
     - offset
     - limit
    """
    pass

  def search(self, source_blog_url, since_timestamp, before_timestamp, ids, source_url):
    """
    Parameters:
     - source_blog_url
     - since_timestamp
     - before_timestamp
     - ids
     - source_url
    """
    pass

  def set_image(self, image):
    """
    Parameters:
     - image
    """
    pass

  def delete_image(self, image_id):
    """
    Parameters:
     - image_id
    """
    pass

  def add_image(self, image):
    """
    Parameters:
     - image
    """
    pass


class Client(Iface):
  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot is not None:
      self._oprot = oprot
    self._seqid = 0

  def get_image(self, id):
    """
    Parameters:
     - id
    """
    self.send_get_image(id)
    return self.recv_get_image()

  def send_get_image(self, id):
    self._oprot.writeMessageBegin('get_image', TMessageType.CALL, self._seqid)
    args = get_image_args()
    args.id = id
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_get_image(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = get_image_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.ex is not None:
      raise result.ex
    raise TApplicationException(TApplicationException.MISSING_RESULT, "get_image failed: unknown result");

  def get_images_since(self, image_id, timestamp, offset, limit):
    """
    Parameters:
     - image_id
     - timestamp
     - offset
     - limit
    """
    self.send_get_images_since(image_id, timestamp, offset, limit)
    return self.recv_get_images_since()

  def send_get_images_since(self, image_id, timestamp, offset, limit):
    self._oprot.writeMessageBegin('get_images_since', TMessageType.CALL, self._seqid)
    args = get_images_since_args()
    args.image_id = image_id
    args.timestamp = timestamp
    args.offset = offset
    args.limit = limit
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_get_images_since(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = get_images_since_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.ex is not None:
      raise result.ex
    raise TApplicationException(TApplicationException.MISSING_RESULT, "get_images_since failed: unknown result");

  def search(self, source_blog_url, since_timestamp, before_timestamp, ids, source_url):
    """
    Parameters:
     - source_blog_url
     - since_timestamp
     - before_timestamp
     - ids
     - source_url
    """
    self.send_search(source_blog_url, since_timestamp, before_timestamp, ids, source_url)
    return self.recv_search()

  def send_search(self, source_blog_url, since_timestamp, before_timestamp, ids, source_url):
    self._oprot.writeMessageBegin('search', TMessageType.CALL, self._seqid)
    args = search_args()
    args.source_blog_url = source_blog_url
    args.since_timestamp = since_timestamp
    args.before_timestamp = before_timestamp
    args.ids = ids
    args.source_url = source_url
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_search(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = search_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.ex is not None:
      raise result.ex
    raise TApplicationException(TApplicationException.MISSING_RESULT, "search failed: unknown result");

  def set_image(self, image):
    """
    Parameters:
     - image
    """
    self.send_set_image(image)
    return self.recv_set_image()

  def send_set_image(self, image):
    self._oprot.writeMessageBegin('set_image', TMessageType.CALL, self._seqid)
    args = set_image_args()
    args.image = image
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_set_image(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = set_image_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.ex is not None:
      raise result.ex
    raise TApplicationException(TApplicationException.MISSING_RESULT, "set_image failed: unknown result");

  def delete_image(self, image_id):
    """
    Parameters:
     - image_id
    """
    self.send_delete_image(image_id)
    return self.recv_delete_image()

  def send_delete_image(self, image_id):
    self._oprot.writeMessageBegin('delete_image', TMessageType.CALL, self._seqid)
    args = delete_image_args()
    args.image_id = image_id
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_delete_image(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = delete_image_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.ex is not None:
      raise result.ex
    raise TApplicationException(TApplicationException.MISSING_RESULT, "delete_image failed: unknown result");

  def add_image(self, image):
    """
    Parameters:
     - image
    """
    self.send_add_image(image)
    return self.recv_add_image()

  def send_add_image(self, image):
    self._oprot.writeMessageBegin('add_image', TMessageType.CALL, self._seqid)
    args = add_image_args()
    args.image = image
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

  def recv_add_image(self, ):
    (fname, mtype, rseqid) = self._iprot.readMessageBegin()
    if mtype == TMessageType.EXCEPTION:
      x = TApplicationException()
      x.read(self._iprot)
      self._iprot.readMessageEnd()
      raise x
    result = add_image_result()
    result.read(self._iprot)
    self._iprot.readMessageEnd()
    if result.success is not None:
      return result.success
    if result.ex is not None:
      raise result.ex
    raise TApplicationException(TApplicationException.MISSING_RESULT, "add_image failed: unknown result");


class Processor(Iface, TProcessor):
  def __init__(self, handler):
    self._handler = handler
    self._processMap = {}
    self._processMap["get_image"] = Processor.process_get_image
    self._processMap["get_images_since"] = Processor.process_get_images_since
    self._processMap["search"] = Processor.process_search
    self._processMap["set_image"] = Processor.process_set_image
    self._processMap["delete_image"] = Processor.process_delete_image
    self._processMap["add_image"] = Processor.process_add_image

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_get_image(self, seqid, iprot, oprot):
    args = get_image_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = get_image_result()
    try:
      result.success = self._handler.get_image(args.id)
    except Exception, ex:
      result.ex = ex
    oprot.writeMessageBegin("get_image", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_get_images_since(self, seqid, iprot, oprot):
    args = get_images_since_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = get_images_since_result()
    try:
      result.success = self._handler.get_images_since(args.image_id, args.timestamp, args.offset, args.limit)
    except Exception, ex:
      result.ex = ex
    oprot.writeMessageBegin("get_images_since", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_search(self, seqid, iprot, oprot):
    args = search_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = search_result()
    try:
      result.success = self._handler.search(args.source_blog_url, args.since_timestamp, args.before_timestamp, args.ids, args.source_url)
    except Exception, ex:
      result.ex = ex
    oprot.writeMessageBegin("search", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_set_image(self, seqid, iprot, oprot):
    args = set_image_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = set_image_result()
    try:
      result.success = self._handler.set_image(args.image)
    except Exception, ex:
      result.ex = ex
    oprot.writeMessageBegin("set_image", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_delete_image(self, seqid, iprot, oprot):
    args = delete_image_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = delete_image_result()
    try:
      result.success = self._handler.delete_image(args.image_id)
    except Exception, ex:
      result.ex = ex
    oprot.writeMessageBegin("delete_image", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

  def process_add_image(self, seqid, iprot, oprot):
    args = add_image_args()
    args.read(iprot)
    iprot.readMessageEnd()
    result = add_image_result()
    try:
      result.success = self._handler.add_image(args.image)
    except Exception, ex:
      result.ex = ex
    oprot.writeMessageBegin("add_image", TMessageType.REPLY, seqid)
    result.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()


# HELPER FUNCTIONS AND STRUCTURES

class get_image_args:
  """
  Attributes:
   - id
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'id', None, None, ), # 1
  )

  def __init__(self, id=None,):
    self.id = id

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.id = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('get_image_args')
    if self.id is not None:
      oprot.writeFieldBegin('id', TType.I32, 1)
      oprot.writeI32(self.id)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class get_image_result:
  """
  Attributes:
   - success
   - ex
  """

  thrift_spec = (
    (0, TType.STRUCT, 'success', (Image, Image.thrift_spec), None, ), # 0
    (1, TType.STRUCT, 'ex', (Exception, Exception.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, ex=None,):
    self.success = success
    self.ex = ex

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.STRUCT:
          self.success = Image()
          self.success.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.ex = Exception()
          self.ex.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('get_image_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.STRUCT, 0)
      self.success.write(oprot)
      oprot.writeFieldEnd()
    if self.ex is not None:
      oprot.writeFieldBegin('ex', TType.STRUCT, 1)
      self.ex.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class get_images_since_args:
  """
  Attributes:
   - image_id
   - timestamp
   - offset
   - limit
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'image_id', None, None, ), # 1
    (2, TType.DOUBLE, 'timestamp', None, None, ), # 2
    (3, TType.I32, 'offset', None, None, ), # 3
    (4, TType.I32, 'limit', None, None, ), # 4
  )

  def __init__(self, image_id=None, timestamp=None, offset=None, limit=None,):
    self.image_id = image_id
    self.timestamp = timestamp
    self.offset = offset
    self.limit = limit

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.image_id = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.DOUBLE:
          self.timestamp = iprot.readDouble();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I32:
          self.offset = iprot.readI32();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.I32:
          self.limit = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('get_images_since_args')
    if self.image_id is not None:
      oprot.writeFieldBegin('image_id', TType.I32, 1)
      oprot.writeI32(self.image_id)
      oprot.writeFieldEnd()
    if self.timestamp is not None:
      oprot.writeFieldBegin('timestamp', TType.DOUBLE, 2)
      oprot.writeDouble(self.timestamp)
      oprot.writeFieldEnd()
    if self.offset is not None:
      oprot.writeFieldBegin('offset', TType.I32, 3)
      oprot.writeI32(self.offset)
      oprot.writeFieldEnd()
    if self.limit is not None:
      oprot.writeFieldBegin('limit', TType.I32, 4)
      oprot.writeI32(self.limit)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class get_images_since_result:
  """
  Attributes:
   - success
   - ex
  """

  thrift_spec = (
    (0, TType.LIST, 'success', (TType.STRUCT,(Image, Image.thrift_spec)), None, ), # 0
    (1, TType.STRUCT, 'ex', (Exception, Exception.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, ex=None,):
    self.success = success
    self.ex = ex

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.LIST:
          self.success = []
          (_etype3, _size0) = iprot.readListBegin()
          for _i4 in xrange(_size0):
            _elem5 = Image()
            _elem5.read(iprot)
            self.success.append(_elem5)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.ex = Exception()
          self.ex.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('get_images_since_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.LIST, 0)
      oprot.writeListBegin(TType.STRUCT, len(self.success))
      for iter6 in self.success:
        iter6.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.ex is not None:
      oprot.writeFieldBegin('ex', TType.STRUCT, 1)
      self.ex.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class search_args:
  """
  Attributes:
   - source_blog_url
   - since_timestamp
   - before_timestamp
   - ids
   - source_url
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'source_blog_url', None, None, ), # 1
    (2, TType.DOUBLE, 'since_timestamp', None, None, ), # 2
    (3, TType.DOUBLE, 'before_timestamp', None, None, ), # 3
    (4, TType.LIST, 'ids', (TType.I32,None), None, ), # 4
    (5, TType.STRING, 'source_url', None, None, ), # 5
  )

  def __init__(self, source_blog_url=None, since_timestamp=None, before_timestamp=None, ids=None, source_url=None,):
    self.source_blog_url = source_blog_url
    self.since_timestamp = since_timestamp
    self.before_timestamp = before_timestamp
    self.ids = ids
    self.source_url = source_url

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.source_blog_url = iprot.readString();
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.DOUBLE:
          self.since_timestamp = iprot.readDouble();
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.DOUBLE:
          self.before_timestamp = iprot.readDouble();
        else:
          iprot.skip(ftype)
      elif fid == 4:
        if ftype == TType.LIST:
          self.ids = []
          (_etype10, _size7) = iprot.readListBegin()
          for _i11 in xrange(_size7):
            _elem12 = iprot.readI32();
            self.ids.append(_elem12)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 5:
        if ftype == TType.STRING:
          self.source_url = iprot.readString();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('search_args')
    if self.source_blog_url is not None:
      oprot.writeFieldBegin('source_blog_url', TType.STRING, 1)
      oprot.writeString(self.source_blog_url)
      oprot.writeFieldEnd()
    if self.since_timestamp is not None:
      oprot.writeFieldBegin('since_timestamp', TType.DOUBLE, 2)
      oprot.writeDouble(self.since_timestamp)
      oprot.writeFieldEnd()
    if self.before_timestamp is not None:
      oprot.writeFieldBegin('before_timestamp', TType.DOUBLE, 3)
      oprot.writeDouble(self.before_timestamp)
      oprot.writeFieldEnd()
    if self.ids is not None:
      oprot.writeFieldBegin('ids', TType.LIST, 4)
      oprot.writeListBegin(TType.I32, len(self.ids))
      for iter13 in self.ids:
        oprot.writeI32(iter13)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.source_url is not None:
      oprot.writeFieldBegin('source_url', TType.STRING, 5)
      oprot.writeString(self.source_url)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class search_result:
  """
  Attributes:
   - success
   - ex
  """

  thrift_spec = (
    (0, TType.LIST, 'success', (TType.STRUCT,(Image, Image.thrift_spec)), None, ), # 0
    (1, TType.STRUCT, 'ex', (Exception, Exception.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, ex=None,):
    self.success = success
    self.ex = ex

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.LIST:
          self.success = []
          (_etype17, _size14) = iprot.readListBegin()
          for _i18 in xrange(_size14):
            _elem19 = Image()
            _elem19.read(iprot)
            self.success.append(_elem19)
          iprot.readListEnd()
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.ex = Exception()
          self.ex.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('search_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.LIST, 0)
      oprot.writeListBegin(TType.STRUCT, len(self.success))
      for iter20 in self.success:
        iter20.write(oprot)
      oprot.writeListEnd()
      oprot.writeFieldEnd()
    if self.ex is not None:
      oprot.writeFieldBegin('ex', TType.STRUCT, 1)
      self.ex.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class set_image_args:
  """
  Attributes:
   - image
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'image', (Image, Image.thrift_spec), None, ), # 1
  )

  def __init__(self, image=None,):
    self.image = image

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.image = Image()
          self.image.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('set_image_args')
    if self.image is not None:
      oprot.writeFieldBegin('image', TType.STRUCT, 1)
      self.image.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class set_image_result:
  """
  Attributes:
   - success
   - ex
  """

  thrift_spec = (
    (0, TType.STRUCT, 'success', (Image, Image.thrift_spec), None, ), # 0
    (1, TType.STRUCT, 'ex', (Exception, Exception.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, ex=None,):
    self.success = success
    self.ex = ex

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.STRUCT:
          self.success = Image()
          self.success.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.ex = Exception()
          self.ex.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('set_image_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.STRUCT, 0)
      self.success.write(oprot)
      oprot.writeFieldEnd()
    if self.ex is not None:
      oprot.writeFieldBegin('ex', TType.STRUCT, 1)
      self.ex.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class delete_image_args:
  """
  Attributes:
   - image_id
  """

  thrift_spec = (
    None, # 0
    (1, TType.I32, 'image_id', None, None, ), # 1
  )

  def __init__(self, image_id=None,):
    self.image_id = image_id

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.I32:
          self.image_id = iprot.readI32();
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('delete_image_args')
    if self.image_id is not None:
      oprot.writeFieldBegin('image_id', TType.I32, 1)
      oprot.writeI32(self.image_id)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class delete_image_result:
  """
  Attributes:
   - success
   - ex
  """

  thrift_spec = (
    (0, TType.BOOL, 'success', None, None, ), # 0
    (1, TType.STRUCT, 'ex', (Exception, Exception.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, ex=None,):
    self.success = success
    self.ex = ex

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.BOOL:
          self.success = iprot.readBool();
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.ex = Exception()
          self.ex.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('delete_image_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.BOOL, 0)
      oprot.writeBool(self.success)
      oprot.writeFieldEnd()
    if self.ex is not None:
      oprot.writeFieldBegin('ex', TType.STRUCT, 1)
      self.ex.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class add_image_args:
  """
  Attributes:
   - image
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRUCT, 'image', (Image, Image.thrift_spec), None, ), # 1
  )

  def __init__(self, image=None,):
    self.image = image

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRUCT:
          self.image = Image()
          self.image.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('add_image_args')
    if self.image is not None:
      oprot.writeFieldBegin('image', TType.STRUCT, 1)
      self.image.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class add_image_result:
  """
  Attributes:
   - success
   - ex
  """

  thrift_spec = (
    (0, TType.STRUCT, 'success', (Image, Image.thrift_spec), None, ), # 0
    (1, TType.STRUCT, 'ex', (Exception, Exception.thrift_spec), None, ), # 1
  )

  def __init__(self, success=None, ex=None,):
    self.success = success
    self.ex = ex

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 0:
        if ftype == TType.STRUCT:
          self.success = Image()
          self.success.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 1:
        if ftype == TType.STRUCT:
          self.ex = Exception()
          self.ex.read(iprot)
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('add_image_result')
    if self.success is not None:
      oprot.writeFieldBegin('success', TType.STRUCT, 0)
      self.success.write(oprot)
      oprot.writeFieldEnd()
    if self.ex is not None:
      oprot.writeFieldBegin('ex', TType.STRUCT, 1)
      self.ex.write(oprot)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
