import logging
import os
import cloudstorage as gcs
import webapp2

from google.appengine.api import app_identity


class Saver:
  def generate_filename(request_id, expected):
    return request_id + u"_" + expected + ".raw"

class FSSaver:
  def save(request_id, expected, data):
      filename =self.generate_filename(request_id, expected)
      dest = os.path.join(self.outdir, filename)
      with open(dest.encode('utf-8'),"ab") as outfile:
        outfile.write(data)

class GCSSaver:

  def __init__():
    default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                            max_delay=5.0,
                                            backoff_factor=2,
                                            max_retry_period=15)
    gcs.set_default_retry_params(default_retry_params)
    self.bucket = '/pagoda_utterances'
  
  def save(request_id, expected, data):
      dest = self.bucket + '/' + self.generate_filename(request_id, expected)
      write_retry_params = gcs.RetryParams(backoff_factor=1.1)
      gcs_file = gcs.open(dest,
                          'ab',
                          content_type='application/octet-stream',
                          retry_params=write_retry_params)
      gcs_file.write(data)
      gcs_file.close()