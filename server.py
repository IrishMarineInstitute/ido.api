#!/usr/bin/python

import tornado.web
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado import gen, httpclient, web, websocket
import json
from datetime import datetime, timedelta

import time
httpclient.AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

class Timeseries():
  time_field = "time"
  poll_frequency = 30000
  default_since_days = 7
  default_until_days = 7
  page_days = 14
  def get_arguments(self,req):
    
    since = req.get_argument("since",None)
    if(since):
      self.since = since
    else:
      d = datetime.today() - timedelta(days=self.default_since_days)
      self.since = "{0}T00:00:00Z".format(d.strftime('%Y-%m-%d'))

    until_ = req.get_argument("until",None)
    if(until_):
      self.until = until_
    else:
      d = datetime.today() + timedelta(days=self.default_until_days)
      self.until = "{0}T00:00:00Z".format(d.strftime('%Y-%m-%d'))

    latmin = req.get_argument("latitude>",None)
    if(latmin):
      self.latmin = float(latmin)  
    latmax = req.get_argument("latitude<",None)
    if(latmax):
      self.latmax = float(latmax)  
    lonmin = req.get_argument("longitude>",None)
    if(lonmin):
      self.lonmin = float(lonmin)  
    lonmax = req.get_argument("longitude<",None)
    if(lonmax):
      self.lonmax = float(lonmax)  
  
  def __init__(self,write,callback=None):
      self.write_message = write
      self.callback = callback

  def get_urls(self):
    urls = []
    end_time = self.since
    while end_time < self.until:
       start_time = end_time
       d1 = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
       d = d1 + timedelta(days=self.default_until_days)
       end_time = "{0}T00:00:00Z".format(d.strftime('%Y-%m-%d'))
       if end_time > self.until:
          end_time = self.until
       urls.append(self.get_url(start_time, end_time))
    
    return urls

  @tornado.gen.engine                                                      
  def cb(self):
    client = tornado.httpclient.AsyncHTTPClient()                        
    for url in self.get_urls():
      response = yield gen.Task(client.fetch, url);
      if response.error:                                                   
        # self.write_message("Error: %s" % response.error) 
        # TODO: logging
        pass
      else:                                                                
        #self.write(response.body)                                        
        json_data = json.loads(response.body)
        for row in json_data["table"]["rows"]:
            newsince = self.since
            o = {};
            for i,col in enumerate(json_data["table"]["columnNames"]):
               o[col] = row[i]
               if col == self.time_field:
                  newsince = row[i]
            if o[self.time_field] > self.since:
              self.write_message(o)
        self.since = newsince
        self.on_page_done()

    if self.callback:
       self.callback()

  def format_params(self,start_time,end_time):
    params = "&time%3E={0}".format(start_time)
    params = "{0}&time<={1}".format(params,end_time)
    if hasattr(self,"latmin"):
      params = "{0}&latitude>={1}".format(params,self.latmin)
    if hasattr(self,"latmax"):
      params = "{0}&latitude<={1}".format(params,self.latmax)
    if hasattr(self,"lonmin"):
      params = "{0}&longitude>={1}".format(params,self.lonmin)
    if hasattr(self,"lonmax"):
      params = "{0}&longitude<={1}".format(params,self.lonmax)
    return params

class IWaveBNetwork(Timeseries):

  def get_url(self,start_time,end_time):
    base = "http://erddap.dm.marine.ie/erddap/tabledap/IWaveBNetwork.json?longitude,latitude,time,station_id,PeakPeriod,PeakDirection,UpcrossPeriod,SignificantWaveHeight,SeaTemperature"
    return "{0}{1}".format(base,self.format_params(start_time,end_time))

class IrishNationalTideGaugeNetwork(Timeseries):
  def get_url(self,start_time,end_time):
    base = "http://erddap.dm.marine.ie/erddap/tabledap/IrishNationalTideGaugeNetwork.json?longitude,latitude,altitude,time,station_id,Water_Level,Water_Level_LAT,Water_Level_OD_Malin,QC_Flag"
    return "{0}{1}".format(base,self.format_params(start_time,end_time))


def get_service_provider(name,write_message):
   if name == "waves":
        return IWaveBNetwork(write_message)

   if name == "tides":
        return IrishNationalTideGaugeNetwork(write_message)

class WSHandler(websocket.WebSocketHandler):
    def check_origin(self, origin):
        return True

    def on_message(self,message):
        pass

    def on_page_done(self):
        pass

    def on_close(self):
        if self.periodic:
           self.periodic.stop()

    @tornado.gen.engine                                                      
    def open(self,name):
        service = get_service_provider(name,self.write_message)
        service.get_arguments(self)
        service.cb()
        periodic = PeriodicCallback(service.cb,self.poll_frequency)
        def quit_at_end():
           if service.since >= service.until:
              periodic.stop()
              self.periodic = None
              self.close()

        service.callback = quit_at_end
        periodic.start()
        self.periodic = periodic

class IndexHandler(tornado.web.RequestHandler):                              

    @tornado.web.asynchronous                                                
    @tornado.gen.engine                                                      
    def get(self,name):                                                           
        def write_message(msg):
            self.write(json.dumps(msg))
            self.write("\n");
        service = get_service_provider(name,write_message)
        service.callback = self.finish
        def flush():
            self.flush()
        service.on_page_done = flush
        service.get_arguments(self)
        service.cb()
    
application = tornado.web.Application([
    (r"/data/mi/(tides|waves)$", IndexHandler),
    (r"/ws/mi/(tides|waves)$", WSHandler),
    ])

application.listen(8080)
IOLoop.instance().start()
