from xml.sax import handler

# SAX handler - track xml path, no attributes
class PathHandler(handler.ContentHandler):
  def __init__(self,track_keys=[],start_keys=[],end_keys=[]):
    self.track_keys = track_keys
    self.start_keys = start_keys
    self.end_keys = end_keys

  def startDocument(self):
    self.path = []

  def endDocument(self):
    pass

  def startElement(self,name,attrs):
    if name in self.track_keys:
      self.path.append(name)

  def endElement(self,name):
    if name in self.track_keys and self.path and self.path[-1] == name:
      self.path.pop()
