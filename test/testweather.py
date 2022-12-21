import  requests
import json
import re
import datetime
import time

#合肥: 101220101 天津:  101030100

SOJSON_WEATHER_URL = "http://t.weather.itboy.net/api/weather/city/101220101"
TODAY=time.strftime("%Y-%m-%d", time.localtime())
TOMORROW = (datetime.datetime.now()+datetime.timedelta(days=1)).strftime("%Y-%m-%d")

def parsemsg(dailymsg):
    high_val = int(re.findall(r"\d+", dailymsg["high"])[0])
    low_val = int(re.findall(r"\d+", dailymsg["low"])[0])
    type = dailymsg["type"]
    return {"high":high_val,"low":low_val,"type":type}

response = requests.get(SOJSON_WEATHER_URL)
msg = response.content.decode("utf-8")
msgjson = json.loads(msg)
forecast = msgjson["data"]["forecast"]
yesterday = msgjson["data"]["yesterday"]
tempdict = {"TODAY":None, "YESTERDAY":None,"TOMORROW":None}
tempdict["YESTERDAY"] = parsemsg(msgjson["data"]["yesterday"])
todaymsg = None
tomorrowmsg = None
for dailymsg in msgjson["data"]["forecast"]:
    datet = dailymsg["ymd"]
    if datet == TODAY:
        tempdict["TODAY"] = parsemsg(msgjson["data"]["yesterday"])
    elif datet == TOMORROW:
        tempdict["TOMORROW"] = parsemsg(msgjson["data"]["yesterday"])

if tempdict["TODAY"]["high"] > 28 and tempdict["TODAY"]["type"] =="晴":
    print("Hi Ting, Today is very hot and sunshine is very strong, please order your lunch")
elif tempdict["TOMORROW"]["high"] > 28 and tempdict["TOMORROW"]["type"] =="晴":
    print("Hi Ting, Tomorrow is very hot and sunshine is very strong, please order your lunch")
