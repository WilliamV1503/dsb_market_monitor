import datetime as dt
import time
import fxmonitor as fx
import commoditiesmonitor as com

def currentTime():
    now=dt.datetime.now()
    now_t=now.strftime("%d %m %y %H %M %S").split()
    return now_t

def isFriday():
    a=dt.datetime.today().weekday()
    if a==4:
        return True
    return False

def isWeekend():
    a=dt.datetime.today().weekday()
    if a==5 or a==6:
        return True
    return False
    
#takes tuple (hour,minute) and sleeps until that time, must be within same day
def sleepTo(t_time):
    now_dt=dt.datetime.now()
    now=now_dt.strftime("%d %m %y %H %M %S").split()
    now_day=int(now[0])
    now_mon=int(now[1])
    now_yr=int(now[2])

    j=dt.datetime.strptime(f"{now_day}/{now_mon}/{now_yr} {t_time[0]}:{t_time[1]}:0", "%d/%m/%y %H:%M:%S")

    t_delta=j-now_dt
    print('Sleeping: ', t_delta, ' To: ', t_time)
    time.sleep(t_delta.total_seconds())
    a=dt.datetime.now()
    print('Slept: ', t_delta, ' To: ', a)
    
#takes tuples (hr,min) start and target, sleeps from start to target 
def sleepNight(start,target):
    h1=24-target[0]
    h2=24-(start[0]+1)
    m1=target[1]
    m2=60-start[1]
    print(h1,h2,m1,m2)
    t_delta=dt.timedelta(hours=h1+h2,minutes=m1+m2)
    print('Sleeping: ', t_delta, ' To: Tomorrow', target)
    time.sleep(t_delta.total_seconds())
    a=dt.datetime.now()
    print('Slept: ', t_delta, ' To: ', a)

#takes tuples (hr,min) start and target, sleeps from start to target over weekend
def sleepWeekend(start,target):
    if target[0]<start[0]:
        h1=target[0]
        h2=24-(start[0]+1)
        m1=target[1]
        m2=60-start[1]
    else:
        h1=target[0]-(start[0]+1)
        h2=0
        m1=target[1]
        m2=60-start[1]
    weekday=dt.datetime.today().weekday()
    n_days=7-weekday
    t_delta=dt.timedelta(days=n_days,hours=h1+h2,minutes=m1+m2)
    print('Sleeping: ', t_delta, ' To: Monday ', target)
    time.sleep(t_delta.total_seconds())
    a=dt.datetime.now()
    print('Slept: ', t_delta, ' To: ', a)
    
def main():
    
    print('MONITOR LAUNCHER INITIATED')
    
    #monitor will sleep until wake_time then remain idle until end_time

    wake_time=(1,52)
    end_time=(1,55)
    
    print(f'Wake time: {wake_time}, \nEnd time: {end_time}')
    
    a=currentTime()
    now=(int(a[3]),int(a[4]))
    
    if isWeekend():
        #sleep until wake_time Monday
        print('Sleeping until wake_time Monday.')
        sleepWeekend(now, wake_time)
    else:
        if now<wake_time:
            #sleep until wake_time that weekday
            print('Sleeping until wake_time.')
            sleepTo(wake_time)
        else:
            #sleep until wake_time next day
            print('Sleeping until wake_time tomorrow.')
            sleepNight(now,wake_time)
    
    while True:
        
        fx.main()
        com.main()
 
        #readjusts time to end time
        print('Sleeping to end_time.')
        sleepTo(end_time)
        
        if isFriday() or isWeekend():
            #sleep until wake time Monday
            print('Sleeping until wake_time Monday.')
            sleepWeekend(end_time, wake_time)
        else:
            #sleep until wake time next day
            print('Sleeping until wake_time tomorrow.')
            sleepNight(end_time,wake_time)
                
main() 