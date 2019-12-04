import logging
import os
import boto
import gcs_oauth2_boto_plugin

logger = logging.getLogger(__name__)

class Saver:
  def generate_filename(self, request_id, expected):
    if isinstance(expected, str):
        expected = unicode(expected, "utf-8")
    return request_id + u"_" + expected + u".raw"

  def flush(self):
    return # noop

class FSSaver(Saver):

  def __init__(self, outdir):
    self.outdir = outdir

  def get_save_path(self, request_id, expected):
      filename = self.generate_filename(request_id, expected)
      dest = os.path.join(self.outdir, filename)
      return dest

  def save(self, request_id, expected, data):
    dest = self.get_save_path(request_id, expected)  

    with open(dest.encode('utf-8'),"ab") as outfile:
        outfile.write(data)

class GCSSaver(Saver):

  def __init__(self, bucket):
    self.bucket = bucket
    self.fssaver = FSSaver("/tmp/")
    logger.info("Initialized Google Cloud Storage saver for bucket %s" % bucket)
  
  def save(self, request_id, expected, data):
    self.expected = expected
    self.request_id = request_id
    self.fssaver.save(request_id, expected, data)

  def flush(self):
    dest = "gs://" + self.bucket + '/' + self.generate_filename(self.request_id, self.expected)
    logger.info("Flushing file to %s" % dest)
    src = self.fssaver.get_save_path(self.request_id, self.expected)

    dst_uri = boto.storage_uri(dest, 'gs')
    with open(src, "rb") as local:
      dst_uri.new_key().set_contents_from_file(local) 
    
