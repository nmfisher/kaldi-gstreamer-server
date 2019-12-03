import logging
import os
import boto
import gcs_oauth2_boto_plugin

class Saver:
  def generate_filename(self, request_id, expected):
    return request_id + u"_" + expected + ".raw"

  def flush(self):
    return # noop

class FSSaver:

  def __init__(self, outdir):
    self.outdir = outdir

  def get_save_path(self, request_id, expected):
      filename = Saver.generate_filename(self, request_id, expected)
      dest = os.path.join(self.outdir, filename)
      return dest

  def save(self, request_id, expected, data):
    dest = self.get_save_path(request_id, expected)  

    with open(dest.encode('utf-8'),"ab") as outfile:
        outfile.write(data)

class GCSSaver:

  def __init__(self, bucket):
#    default_retry_params = gcs.RetryParams(initial_delay=0.2,
#                                            max_delay=5.0,
#                                            backoff_factor=2,
#                                            max_retry_period=15)
#    gcs.set_default_retry_params(default_retry_params)
    self.bucket = bucket
    self.fssaver = FSSaver("/tmp/")
  
  def save(self, request_id, expected, data):
    self.expected = expected
    self.request_id = request_id
    self.fssaver.save(request_id, expected, data)

  def flush(self):
    dest = self.bucket + '/' + Saver.generate_filename(self, self.request_id, self.expected)
    src = self.fssaver.get_save_path(self.request_id, self.expected)

    #local = open(src, "rb")
    #print(memoryview(local))

    dst_uri = boto.storage_uri(dest, 'gs')
    #print(type(local))
    print(dest)
    with open(src, "rb") as local:
      dst_uri.new_key().set_contents_from_file(local) 
    
